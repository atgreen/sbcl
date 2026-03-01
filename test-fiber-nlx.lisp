;;; test-fiber-nlx.lisp --- Test non-local exit across fiber/carrier boundary
;;;
;;; This tests the edge case where a closure capturing a BLOCK name
;;; (or TAGBODY tag) established on the carrier thread's stack is
;;; called from within a fiber running on that carrier.
;;;
;;; The concern: SBCL's unwind machinery uses direct stack-frame
;;; pointers.  The fiber's *current-unwind-protect-block* chain is
;;; separate from the carrier's (swapped on yield/resume).  When
;;; RETURN-FROM fires, the assembly routine compares the fiber's UWP
;;; chain against the target block's saved UWP (captured from the
;;; carrier), and they won't match.  Depending on the UWP state, this
;;; could cause:
;;;   - Silent stack corruption (if both UWP chains happen to be 0)
;;;   - Segfault (if the mismatch causes a NULL UWP dereference)
;;;   - Undefined behavior in the scheduler and GC metadata
;;;
;;; Usage: sbcl --load test-fiber-nlx.lisp
;;;
;;; Expected behavior with a correct implementation: each test should
;;; produce a clean error ("block does not exist" or similar) rather
;;; than corruption or segfault.

(in-package :cl-user)

(defvar *escape-closure* nil)
(defvar *go-closure* nil)
(defvar *test-count* 0)
(defvar *pass-count* 0)

(defmacro with-test ((name) &body body)
  `(progn
     (incf *test-count*)
     (format t "~%=== ~A ===~%" ,name)
     (force-output)
     (handler-case
         (progn ,@body)
       (serious-condition (c)
         (format t "Got condition: ~A (~A)~%" (type-of c) c)
         (force-output)))))

(defun report-pass (description)
  (incf *pass-count*)
  (format t "  PASS: ~A~%" description)
  (force-output))

(defun report-fail (description detail)
  (format t "  FAIL: ~A -- ~A~%" description detail)
  (force-output))

;;; ---------------------------------------------------------------
;;; Test 1: RETURN-FROM carrier BLOCK, no unwind-protects anywhere.
;;;
;;; This is the simplest case.  The fiber's UWP is 0 (new fiber,
;;; no unwind-protects).  If the carrier's BLOCK was also entered
;;; with UWP=0, the unwind machinery sees a match and does DO-EXIT,
;;; which restores RBP/RSP to the carrier's stack frame -- silently
;;; abandoning the fiber and corrupting the scheduler.
;;;
;;; If the carrier side has UWPs (from the scheduler internals),
;;; the mismatch causes the routine to walk from UWP=0, which
;;; dereferences NULL.
;;; ---------------------------------------------------------------
(with-test ("Test 1: RETURN-FROM carrier BLOCK from fiber (no UWPs)")
  (let ((result
          (block carrier-block
            (setf *escape-closure*
                  (lambda () (return-from carrier-block :escaped)))
            (let ((fiber-results
                    (sb-thread:run-fibers
                      (list
                        (sb-thread:make-fiber
                          (lambda ()
                            (format t "  Fiber running, calling RETURN-FROM...~%")
                            (force-output)
                            (funcall *escape-closure*)
                            ;; Should not reach here
                            :fiber-completed))))))
              (list :normal-return fiber-results)))))
    (cond
      ((eq result :escaped)
       (report-fail "BLOCK was exited from fiber"
                    "unwind crossed fiber/carrier boundary without error"))
      ((and (consp result) (eq (first result) :normal-return))
       (let ((fiber-result (first (second result))))
         (cond
           ((eq fiber-result :fiber-completed)
            (report-fail "RETURN-FROM silently failed"
                         "fiber completed normally (closure was a no-op?)"))
           ((typep fiber-result 'condition)
            (report-pass
              (format nil "fiber got error: ~A" (type-of fiber-result))))
           (t
            (report-fail "unexpected fiber result" fiber-result)))))
      (t
       (report-fail "unexpected result" result)))))

;;; ---------------------------------------------------------------
;;; Test 2: RETURN-FROM with carrier-side UNWIND-PROTECT.
;;;
;;; The carrier has a UWP block on its stack.  The target block's
;;; saved UWP points to this carrier UWP.  The fiber's current UWP
;;; is 0.  Mismatch → the unwind routine tries to walk from 0,
;;; which means dereferencing NULL.
;;; ---------------------------------------------------------------
(with-test ("Test 2: RETURN-FROM with carrier-side UNWIND-PROTECT")
  (let ((cleanup-ran nil))
    (let ((result
            (block carrier-block
              (unwind-protect
                   (progn
                     (setf *escape-closure*
                           (lambda ()
                             (return-from carrier-block :escaped)))
                     (let ((fiber-results
                             (sb-thread:run-fibers
                               (list
                                 (sb-thread:make-fiber
                                   (lambda ()
                                     (format t "  Fiber running, calling RETURN-FROM...~%")
                                     (force-output)
                                     (funcall *escape-closure*)
                                     :fiber-completed))))))
                       (list :normal-return fiber-results)))
                (setf cleanup-ran t)))))
      (format t "  cleanup-ran: ~S~%" cleanup-ran)
      (cond
        ((eq result :escaped)
         (report-fail "BLOCK was exited from fiber"
                      "unwind crossed boundary and ran carrier cleanup"))
        ((and (consp result) (eq (first result) :normal-return))
         (let ((fiber-result (first (second result))))
           (if (typep fiber-result 'condition)
               (report-pass
                 (format nil "fiber got error: ~A" (type-of fiber-result)))
               (report-fail "unexpected fiber result" fiber-result))))
        (t
         (report-fail "unexpected result" result))))))

;;; ---------------------------------------------------------------
;;; Test 3: RETURN-FROM with fiber-side UNWIND-PROTECT.
;;;
;;; The fiber has its own UWP block.  The unwind routine walks
;;; the fiber's UWP chain (running the fiber's cleanup form),
;;; then may reach 0 and either match the target or dereference NULL.
;;; ---------------------------------------------------------------
(with-test ("Test 3: RETURN-FROM with fiber-side UNWIND-PROTECT")
  (let ((result
          (block carrier-block
            (setf *escape-closure*
                  (lambda () (return-from carrier-block :escaped)))
            (let ((fiber-results
                    (sb-thread:run-fibers
                      (list
                        (sb-thread:make-fiber
                          (lambda ()
                            (let ((fiber-cleanup-ran nil))
                              (unwind-protect
                                   (progn
                                     (format t "  Fiber running, calling RETURN-FROM...~%")
                                     (force-output)
                                     (funcall *escape-closure*)
                                     :fiber-completed)
                                (setf fiber-cleanup-ran t)
                                (format t "  Fiber cleanup ran: ~S~%"
                                        fiber-cleanup-ran)
                                (force-output)))))))))
              (list :normal-return fiber-results)))))
    (cond
      ((eq result :escaped)
       (report-fail "BLOCK was exited from fiber"
                    "unwind crossed boundary, ran fiber cleanup, jumped to carrier"))
      ((and (consp result) (eq (first result) :normal-return))
       (let ((fiber-result (first (second result))))
         (if (typep fiber-result 'condition)
             (report-pass
               (format nil "fiber got error: ~A" (type-of fiber-result)))
             (report-fail "unexpected fiber result" fiber-result))))
      (t
       (report-fail "unexpected result" result)))))

;;; ---------------------------------------------------------------
;;; Test 4: GO to a carrier-established TAGBODY tag from fiber.
;;;
;;; Same mechanism as RETURN-FROM (both use unwind-blocks), but
;;; exercises the TAGBODY/GO path.
;;; ---------------------------------------------------------------
(with-test ("Test 4: GO to carrier TAGBODY tag from fiber")
  (let ((reached-end nil)
        (reached-tag nil))
    (tagbody
       (setf *go-closure* (lambda () (go carrier-tag)))
       (let ((fiber-results
               (sb-thread:run-fibers
                 (list
                   (sb-thread:make-fiber
                     (lambda ()
                       (format t "  Fiber running, calling GO...~%")
                       (force-output)
                       (funcall *go-closure*)
                       :fiber-completed))))))
         (declare (ignore fiber-results)))
       (setf reached-end t)
       (go done)
     carrier-tag
       (setf reached-tag t)
       (format t "  Reached carrier-tag via GO from fiber~%")
     done)
    (cond
      (reached-tag
       (report-fail "GO crossed fiber/carrier boundary"
                    "jumped to carrier tagbody tag from fiber"))
      (reached-end
       (report-pass "GO did not cross boundary (fiber caught the error)"))
      (t
       (report-fail "unexpected control flow" "neither tag nor end reached")))))

;;; ---------------------------------------------------------------
;;; Test 5: RETURN-FROM with (OPTIMIZE (SAFETY 0)).
;;;
;;; At safety 0, SBCL may skip the value-cell indirection and use
;;; a direct unwind-block pointer.  The NULL check still exists in
;;; the assembly, but the value-cell zeroing on block exit is
;;; skipped.  If the block is still in dynamic extent (as it is
;;; here), the pointer is valid regardless.
;;; ---------------------------------------------------------------
(with-test ("Test 5: RETURN-FROM at (SAFETY 0)")
  (let ((result
          (block carrier-block
            (setf *escape-closure*
                  (locally (declare (optimize (safety 0)))
                    (lambda () (return-from carrier-block :escaped))))
            (let ((fiber-results
                    (sb-thread:run-fibers
                      (list
                        (sb-thread:make-fiber
                          (lambda ()
                            (format t "  Fiber running (safety 0), calling RETURN-FROM...~%")
                            (force-output)
                            (funcall *escape-closure*)
                            :fiber-completed))))))
              (list :normal-return fiber-results)))))
    (cond
      ((eq result :escaped)
       (report-fail "BLOCK was exited from fiber (safety 0)"
                    "unwind crossed boundary without error"))
      ((and (consp result) (eq (first result) :normal-return))
       (let ((fiber-result (first (second result))))
         (if (typep fiber-result 'condition)
             (report-pass
               (format nil "fiber got error: ~A" (type-of fiber-result)))
             (report-fail "unexpected fiber result" fiber-result))))
      (t
       (report-fail "unexpected result" result)))))

;;; ---------------------------------------------------------------
;;; Summary
;;; ---------------------------------------------------------------
(format t "~%=== Summary: ~D/~D tests passed ===~%"
        *pass-count* *test-count*)
(force-output)
