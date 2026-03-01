;;; test-fiber-nlx-diag.lisp --- Diagnostic version
;;;
;;; Usage: ./run-sbcl.sh --load test-fiber-nlx-diag.lisp

(in-package :cl-user)

(defvar *escape-closure* nil)
(defvar *go-closure* nil)

(format t "~%SBCL version: ~A~%" (lisp-implementation-version))
(format t "Features include :sb-fiber? ~A~%"
        (if (member :sb-fiber *features*) "YES" "NO"))
(force-output)

(defun run-diag (name thunk)
  (format t "~%=== ~A ===~%" name)
  (force-output)
  (multiple-value-bind (result err)
      (ignore-errors (funcall thunk))
    (if err
        (format t "  Top-level error: ~A (~A)~%" (type-of err) err)
        (format t "  Final result: ~S~%" result))
    (force-output)))

;;; Diag 1: RETURN-FROM carrier BLOCK — detailed
(run-diag "Diag 1: RETURN-FROM — detailed"
  (lambda ()
    (let ((calling-thread sb-thread:*current-thread*))
      (block carrier-block
        (setf *escape-closure*
              (lambda () (return-from carrier-block :escaped)))
        (format t "  Calling thread: ~A~%" calling-thread)
        (force-output)
        (let ((fiber-results
                (sb-thread:run-fibers
                  (list
                    (sb-thread:make-fiber
                      (lambda ()
                        (let ((ft sb-thread:*current-thread*))
                          (format t "  Fiber thread: ~A~%" ft)
                          (format t "  Same thread? ~A~%" (eq ft calling-thread))
                          (force-output))
                        (format t "  About to call RETURN-FROM closure...~%")
                        (force-output)
                        (multiple-value-bind (v e)
                            (ignore-errors (funcall *escape-closure*))
                          (if e
                              (progn
                                (format t "  Fiber caught: ~A (~A)~%" (type-of e) e)
                                (force-output)
                                (list :fiber-error (type-of e)))
                              (progn
                                (format t "  Closure returned: ~S~%" v)
                                (force-output)
                                (list :closure-returned v))))))))))
          (format t "  run-fibers returned: ~S~%" fiber-results)
          (force-output)
          (list :normal-return fiber-results))))))

;;; Diag 2: Single carrier
(run-diag "Diag 2: RETURN-FROM — single carrier"
  (lambda ()
    (let ((calling-thread sb-thread:*current-thread*))
      (block carrier-block
        (setf *escape-closure*
              (lambda () (return-from carrier-block :escaped)))
        (let ((fiber-results
                (sb-thread:run-fibers
                  (list
                    (sb-thread:make-fiber
                      (lambda ()
                        (format t "  Same thread? ~A~%"
                                (eq sb-thread:*current-thread* calling-thread))
                        (force-output)
                        (multiple-value-bind (v e)
                            (ignore-errors (funcall *escape-closure*))
                          (if e
                              (progn
                                (format t "  Fiber caught: ~A (~A)~%" (type-of e) e)
                                (list :error (type-of e)))
                              (progn
                                (format t "  Closure returned: ~S~%" v)
                                (list :returned v)))))))
                  :carrier-count 1)))
          (format t "  run-fibers returned: ~S~%" fiber-results)
          (force-output)
          (list :normal-return fiber-results))))))

;;; Diag 3: GO
(run-diag "Diag 3: GO — detailed"
  (lambda ()
    (let ((reached-tag nil))
      (tagbody
         (setf *go-closure* (lambda () (go carrier-tag)))
         (let ((fiber-results
                 (sb-thread:run-fibers
                   (list
                     (sb-thread:make-fiber
                       (lambda ()
                         (format t "  About to call GO closure...~%")
                         (force-output)
                         (multiple-value-bind (v e)
                             (ignore-errors (funcall *go-closure*))
                           (if e
                               (progn
                                 (format t "  Fiber caught: ~A (~A)~%" (type-of e) e)
                                 (list :error (type-of e)))
                               (progn
                                 (format t "  GO returned: ~S~%" v)
                                 (list :returned v))))))))))
           (format t "  run-fibers returned: ~S~%" fiber-results)
           (force-output))
         (go done)
       carrier-tag
         (setf reached-tag t)
         (format t "  Reached carrier-tag!~%")
       done)
      (format t "  reached-tag: ~S~%" reached-tag)
      (force-output)
      (list :done :reached-tag reached-tag))))

;;; Diag 4: THROW to carrier CATCH tag (baseline)
(run-diag "Diag 4: THROW to carrier CATCH (baseline)"
  (lambda ()
    (catch 'carrier-tag
      (sb-thread:run-fibers
        (list
          (sb-thread:make-fiber
            (lambda ()
              (format t "  About to THROW...~%")
              (force-output)
              (multiple-value-bind (v e)
                  (ignore-errors (throw 'carrier-tag :thrown-value))
                (if e
                    (progn
                      (format t "  Fiber caught: ~A (~A)~%" (type-of e) e)
                      (list :error (type-of e)))
                    (progn
                      (format t "  THROW returned: ~S~%" v)
                      (list :returned v)))))))))))

;;; Diag 5: Post-mortem fiber state
(run-diag "Diag 5: Post-mortem fiber state"
  (lambda ()
    (let ((test-fiber nil))
      (block carrier-block
        (setf *escape-closure*
              (lambda () (return-from carrier-block :escaped)))
        (setf test-fiber
              (sb-thread:make-fiber
                (lambda ()
                  (funcall *escape-closure*)
                  :completed)))
        (let ((fiber-results (sb-thread:run-fibers (list test-fiber))))
          (format t "  Fiber state: ~S~%" (sb-thread:fiber-state test-fiber))
          (format t "  Fiber result: ~S (type: ~A)~%"
                  (sb-thread:fiber-result test-fiber)
                  (type-of (sb-thread:fiber-result test-fiber)))
          (format t "  Fiber alive? ~S~%" (sb-thread:fiber-alive-p test-fiber))
          (force-output)
          (list :normal-return fiber-results))))))

(format t "~%=== Done ===~%")
(force-output)
