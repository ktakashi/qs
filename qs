#!/bin/sh
#| -*- mode:scheme; coding:utf-8; -*-
exec sagittarius $0 "$@"
|#
(import (rnrs)
	(dbi)
	(clos user)
	(text sql)
	(sagittarius control)
	(srfi :13)
	(srfi :39)
	(srfi :117)
	(match)
	(getopt))

(define *debug* (make-parameter #f))
(define *delimiter* (make-parameter #\space))

(define-syntax debug
  (syntax-rules ()
    ((_ expr ...)
     (when (*debug*)
       (write expr (current-error-port)) ...
       (newline (current-error-port))))))

(define make-temporary-table
  (let ((count 0))
    (lambda ()
      (set! count (+ count 1))
      (string->symbol (string-append "temp_" (number->string count))))))

(define-constant +stdin+ 'stdin)

(define (join? x) (string-contains (symbol->string x) "join"))
(define (stdin? x) (eq? x +stdin+))

(define (analyse-sql! ssql queue)
  (define (->temporary-table table)
    (match table
      (('! t)
       (let ((temp (make-temporary-table))
	     (table (string->symbol t)))
	 (list-queue-add-front! queue (cons table temp))
	 temp))
      ((? stdin?)
       (let ((temp (make-temporary-table)))
	 (list-queue-add-front! queue (cons table temp))
	 temp))
      ((? symbol? x) x)
      ((t1 ((? join? x) t2 rest ...))
       (list (->temporary-table t1)
	     (cons* x (->temporary-table t2) rest)))
      (('as t alias)
       (let ((temp (->temporary-table t)))
	 (list 'as temp alias)))))
  (match ssql
    (('from tables ...) (cons 'from (map ->temporary-table tables)))
    ((a . d) (cons (analyse-sql! a queue) (analyse-sql! d queue)))
    (v v)))

(define (read-delimited-line p :optional (count #f))
  (define delim (*delimiter*))
  ;; copied from (text csv)
  (define (read-entry p)
    (define (finish cs eol?)
      (values (list->string (reverse! cs)) eol?))
    (define (crlf? c p)
      (case c
	((#\return)
	 (when (eqv? (lookahead-char p) #\newline)
	   (get-char p))
	 #t)
	((#\newline) #t)
	(else #f)))
    ;; reads until #\,
    (let loop ((cs '()) (q? #f))
      (let ((c (get-char p)))
	(cond ((eof-object? c) (finish cs #t)) ;; EOF end 
	      ((and (not q?) (eqv? c delim)
		    (if (eqv? delim #\space)
			(and (not (for-all (lambda (c) (char=? c delim)) cs))
			     (not (eqv? (lookahead-char p) delim)))
			#t))
	       (finish cs #f)) ;; end
	      ((eqv? c #\") ;; double quote
	       (if q?
		   (case (lookahead-char p)
		     ((#\") (get-char p) (loop (cons c cs) q?))
		     (else (loop cs #f)))
		   (loop cs #t)))
	      ((and (not q?) (crlf? c p)) (finish cs #t))
	      (else (loop (cons c cs) q?))))))
  (let loop ((r '()))
    (let ((c (lookahead-char p)))
      (cond ((eof-object? c) c)
	    ((and count (eqv? (length r) (- count 1)))
	     (map string-trim-both (reverse! (cons (get-line p) r))))
	    ((eqv? c #\#) (get-line p) (loop r))
	    (else
	     (let-values (((e eol?) (read-entry p)))
	       (if eol?
		   (map string-trim-both (reverse! (cons e r)))
		   (loop (cons e r)))))))))


(define (populate-tables dbi-conn skip-header queue seen)
  (define (create-table table row named?)
    (define (anon row)
      (define (gen i) (string->symbol (format "col~a" i)))
      (do ((i 0 (+ i 1)) (r row (cdr r)) (col '() (cons (gen i) col)))
	  ((null? r) (reverse! col))))
    (let* ((columns (if named? row (anon row)))
	   (ssql `(create-table ,table
				,(map (lambda (c) (list c 'text)) columns)))
	   (sql (ssql->sql ssql)))
      (debug "Creating: " sql)
      (dbi-execute-query-using-connection! dbi-conn sql)
      (values row columns)))

  (define (insert-data stmt row)
    (apply dbi-execute! stmt row))
  
  (define (populate-port in table)
    (let-values (((r columns)
		  (create-table table (read-delimited-line in) skip-header)))
      (let* ((insert `(insert-into ,table
				   ,columns
				   (values ,(map (lambda (_) '?) columns))))
	     (stmt (dbi-prepare dbi-conn (ssql->sql insert)))
	     (len (length columns)))
	(unless skip-header (insert-data stmt r))
	(do ((r (read-delimited-line in len) (read-delimited-line in len)))
	    ((eof-object? r))
	  (insert-data stmt r)))))
  
  (define (populate-table info)
    (let ((file (car info))
	  (table (cdr info)))
      (unless (hashtable-contains? seen file)
	(hashtable-set! seen file #t)
	(if (eq? file +stdin+)
	    (populate-port (current-input-port) table)
	    (call-with-input-file (symbol->string file)
	      (lambda (in) (populate-port in table)))))))
  (for-each populate-table (list-queue-list queue)))

(define (dump-result show-column? query)
  (define (show-vector v)
    (do ((i 0 (+ i 1)))
	((= i (vector-length v)) (newline))
      (unless (zero? i) (display #\,))
      (display (vector-ref v i))))
  (when (is-a? query <dbi-query>)
    (when show-column? (show-vector (dbi-columns query)))
    (dbi-do-fetch! (c query) (show-vector c))))

(define (usage)
  (define (printe . args)
    (for-each (lambda (v)
		(display v (current-error-port))
		(newline (current-error-port))) args))
  (printe "qs [OPTIONS] SQL")
  (printe "[OPTIONS]")
  (printe " -h, --help    show this help and exit")
  (printe " -V, --verbose show debug messages")
  (printe)
  (printe " Input data options: ")
  (printe "   -H, --skip-header")
  (printe "        Skip header row")
  (printe "   -d, --delimitor")
  (printe "        Delimitor of the given data. Defalt space")
  (printe "        If the delimitor is a space, then the data reader skips")
  (printe "        continued spaces. e.g. 'aa    bb' is read 'aa' and 'bb'")
  (printe)
  (printe " Output data options: ")
  (printe "   -O, --output-header")
  (printe "        Show headers")
  (exit -1))

(define (main args)
  (with-args (cdr args)
      ((skip-header   (#\H "skip-header") #f #f)
       (delimiter     (#\d "delimiter") #t #f)
       (output-header (#\O "output-header") #f #f)
       (help          (#\h "help") #f #f)
       (verbose       (#\V "verbose") #f #f)
       ;; TODO more
       . rest)
    (when (or help (null? rest)) (usage))
    (*debug* verbose)
    (when delimiter (*delimiter* (string-ref delimiter 0)))
    (let* ((sql (car rest))
	   (ssql (sql->ssql (open-string-input-port sql) :simplify #t))
	   (conn (dbi-connect "dbi:sqlite3:database=:memory:"))
	   (seen (make-eq-hashtable)))
      (debug "INPUT: " sql)
      (unwind-protect
       ;; skip *TOP*
       (dolist (s (cdr ssql))
	 (debug "SSQL: " s)
	 (let* ((table-queue (list-queue))
		(invoking-sql (analyse-sql! s table-queue)))
	   (debug "Converted SSQL: " invoking-sql)
	   (unless (list-queue-empty? table-queue)
	     (populate-tables conn skip-header table-queue seen))
	   (dump-result output-header
	    (dbi-execute-query-using-connection!
	     conn (ssql->sql invoking-sql)))))
       (dbi-close conn))
      (exit 0))))


