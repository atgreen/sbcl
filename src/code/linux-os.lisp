;;;; OS interface functions for SBCL under Linux

;;;; This software is part of the SBCL system. See the README file for
;;;; more information.
;;;;
;;;; This software is derived from the CMU CL system, which was
;;;; written at Carnegie Mellon University and released into the
;;;; public domain. The software is in the public domain and is
;;;; provided with absolutely no warranty. See the COPYING and CREDITS
;;;; files for more information.

(in-package "SB-SYS")

;;; Check that target machine features are set up consistently with
;;; this file.
#-linux (error "missing :LINUX feature")

(defun software-type ()
  "Return a string describing the supporting software."
  "Linux")

;;; Return user time, system time, and number of page faults.
(defun get-system-info ()
  (multiple-value-bind
      (err? utime stime maxrss ixrss idrss isrss minflt majflt)
      (sb-unix:unix-getrusage sb-unix:rusage_self)
    (declare (ignore maxrss ixrss idrss isrss minflt))
    (unless err? ; FIXME: nonmnemonic (reversed) name for ERR?
      (error "Unix system call getrusage failed: ~A." (strerror utime)))
    (values utime stime majflt)))

;;;; ===== Cgroup-aware CPU count =====
;;;
;;; Detect available CPUs respecting cgroup limits (v1 and v2).
;;; Used by the fiber scheduler to default carrier-count.

(defun %read-first-line (path)
  "Read the first line from PATH, or NIL if the file doesn't exist or can't be read."
  (handler-case
      (with-open-file (s path :if-does-not-exist nil)
        (when s (read-line s nil nil)))
    (error () nil)))

(defun %parse-cpu-range (str)
  "Parse a cpuset range string like \"0-3,8-15\" and return the count of CPUs.
Returns NIL if STR is nil or empty."
  (when (and str (plusp (length str)))
    (let ((count 0)
          (start 0)
          (len (length str)))
      (flet ((parse-range (begin end)
               (let* ((piece (string-trim " " (subseq str begin end)))
                      (dash (position #\- piece)))
                 (if dash
                     (let ((lo (parse-integer piece :end dash :junk-allowed t))
                           (hi (parse-integer piece :start (1+ dash) :junk-allowed t)))
                       (when (and lo hi)
                         (incf count (1+ (- hi lo)))))
                     (when (parse-integer piece :junk-allowed t)
                       (incf count 1))))))
        (loop for i from 0 below len
              when (char= (char str i) #\,)
              do (parse-range start i) (setf start (1+ i)))
        (parse-range start len))
      (if (plusp count) count nil))))

(defun %cgroup-v2-cpu-limit ()
  "Read cgroup v2 cpu.max and return ceil(quota/period), or NIL."
  (let ((line (%read-first-line "/sys/fs/cgroup/cpu.max")))
    (when line
      (let ((space (position #\Space line)))
        (when space
          (let ((quota-str (subseq line 0 space))
                (period-str (string-trim " "
                                         (subseq line (1+ space)))))
            (unless (string= quota-str "max")
              (let ((quota (parse-integer quota-str :junk-allowed t))
                    (period (parse-integer period-str :junk-allowed t)))
                (when (and quota period (plusp quota) (plusp period))
                  (ceiling quota period))))))))))

(defun %cgroup-v1-cpu-limit ()
  "Read cgroup v1 cpu.cfs_quota_us/cpu.cfs_period_us, or NIL.
Tries the standard mount path first."
  (let ((quota-str (%read-first-line "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"))
        (period-str (%read-first-line "/sys/fs/cgroup/cpu/cpu.cfs_period_us")))
    (when (and quota-str period-str)
      (let ((quota (parse-integer (string-trim " " quota-str)
                                  :junk-allowed t))
            (period (parse-integer (string-trim " " period-str)
                                   :junk-allowed t)))
        (when (and quota period (plusp quota) (plusp period))
          (ceiling quota period))))))

(defun %cgroup-cpuset-count ()
  "Read cpuset CPU list from cgroup v2 or v1, return count or NIL."
  (let ((line (or (%read-first-line "/sys/fs/cgroup/cpuset.cpus.effective")
                  (%read-first-line "/sys/fs/cgroup/cpuset/cpuset.cpus"))))
    (when line
      (%parse-cpu-range line))))

(defun %cgroup-effective-cpus ()
  "Return the cgroup-constrained CPU count, or NIL if no limits apply.
Checks v2 first (cpu.max, cpuset.cpus.effective), then v1."
  (let ((v2 (probe-file "/sys/fs/cgroup/cgroup.controllers"))
        (cpu-limit nil)
        (cpuset-limit nil))
    (if v2
        ;; cgroup v2
        (setf cpu-limit (%cgroup-v2-cpu-limit)
              cpuset-limit (%cgroup-cpuset-count))
        ;; cgroup v1
        (setf cpu-limit (%cgroup-v1-cpu-limit)
              cpuset-limit (%cgroup-cpuset-count)))
    (cond
      ((and cpu-limit cpuset-limit) (min cpu-limit cpuset-limit))
      (cpu-limit cpu-limit)
      (cpuset-limit cpuset-limit)
      (t nil))))

;;; support for CL:MACHINE-VERSION defined OAOO elsewhere
(defun get-machine-version ()
  (or
   #+(and mips little-endian)
   "little-endian"
   #+(and mips big-endian)
   "big-endian"
   (let ((marker
          ;; hoping "cpu" exists and gives something useful in
          ;; all relevant Linuxen...
          ;;
          ;; from Lars Brinkhoff sbcl-devel 26 Jun 2003:
          ;;   I examined different versions of Linux/PPC at
          ;;   http://lxr.linux.no/ (the file that outputs
          ;;   /proc/cpuinfo is arch/ppc/kernel/setup.c, if
          ;;   you want to check), and all except 2.0.x
          ;;   seemed to do the same thing as far as the
          ;;   "cpu" field is concerned, i.e. it always
          ;;   starts with the (C-syntax) string "cpu\t\t: ".
          #+(or ppc ppc64) "cpu"
          ;; The field "model name" exists on kernel 2.4.21-rc6-ac1
          ;; anyway, with values e.g.
          ;;   "AMD Athlon(TM) XP 2000+"
          ;;   "Intel(R) Pentium(R) M processor 1300MHz"
          ;; which seem comparable to the information in the example
          ;; in the MACHINE-VERSION page of the ANSI spec.
          #+(or x86 x86-64) "model name"
          #+(or arm arm64) "Processor"))
     (when marker
       (with-open-file (stream "/proc/cpuinfo"
                               ;; Even on Linux it's an option to build
                               ;; kernels without /proc filesystems, so
                               ;; degrade gracefully.
                               :if-does-not-exist nil)
         (loop with line while (setf line (read-line stream nil))
               when (eql (search marker line) 0)
               return (string-trim " " (subseq line (1+ (position #\: line))))))))))
