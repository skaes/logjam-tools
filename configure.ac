AC_INIT([logjam-tools], [0.1], [skaes@railsexpress.de],
        [logjam-tools], [https://github.com/skaes/logjam-tools])
AC_PREREQ([2.59])
AM_INIT_AUTOMAKE([1.10 -Wall no-define foreign])

AC_CONFIG_MACRO_DIR([m4])

# Check for pkg-config program, used for configuring some libraries.
#

m4_define_default([PKG_PROG_PKG_CONFIG],
[AC_MSG_CHECKING([pkg-config])
AC_MSG_RESULT([no])])

PKG_PROG_PKG_CONFIG

# If the pkg-config autoconf support isn't installed, define its
# autoconf macro to disable any packages depending on it.
#
m4_define_default([PKG_CHECK_MODULES],
[AC_MSG_CHECKING([$1])
AC_MSG_RESULT([no])
$4])

# AC_CONFIG_HEADERS([config.h])

OLD_CFLAGS=$CFLAGS
test -z "$OLD_CFLAGS" && OLD_CFLAGS="-O3 -ggdb"
OLD_CC=$CC
test -z "$OLD_CC" && test `uname -s` = "Darwin" && OLD_CC="clang"
AC_PROG_CXX(clang++ g++ c++)
AC_PROG_CXXCPP
test -z "$OLD_CFLAGS" || CFLAGS=$OLD_CFLAGS
test -z "$OLD_CC" || CC=$OLD_CC

AC_CONFIG_FILES([Makefile src/Makefile go/Makefile])
AC_CONFIG_HEADERS([config.h])

AC_ARG_WITH(opt-dir,
	AS_HELP_STRING([--with-opt-dir=DIR-LIST],
		       [add optional headers and libraries directories separated by $PATH_SEPARATOR]),
	[
		val=`echo "$PATH_SEPARATOR$withval" | sed "s|$PATH_SEPARATOR\([[^$PATH_SEPARATOR]*]\)| -I\1/include -I\1/include/libbson-1.0 -I\1/include/libmongoc-1.0|g;s/^ //"`
		OPTDIR_CPPFLAGS="$val"
		val=`echo "$PATH_SEPARATOR$withval" | sed "s|$PATH_SEPARATOR\([[^$PATH_SEPARATOR]*]\)| -L\1/lib -Wl,-rpath -Wl,\1/lib|g;s/^ //"`
		OPTDIR_LDFLAGS="$val"
                AC_SUBST([OPTDIR_CPPFLAGS])
		AC_SUBST([OPTDIR_LDFLAGS])
                AC_SUBST([DEPS_LIBS],["-lczmq -lzmq -ljson-c -lmongoc-1.0 -lbson-1.0 -lsnappy"])
	])

AS_IF([test "x$prefix" != "x"],
      [ PKG_CONFIG_PATH="$prefix/lib/pkgconfig:$PKG_CONFIG_PATH"
        export PKG_CONFIG_PATH
        AC_SUBST([PKG_CONFIG_PATH], [$PKG_CONFIG_PATH])
        AC_SUBST([OPTDIR_LDFLAGS], ["-Wl,-rpath -Wl,$libdir"])
      ])

AS_IF([test "x$with_opt_dir" == "x"],
      [
        PKG_CHECK_MODULES([DEPS],[libzmq >= 4.1.5 libczmq >= 4.2.0 json-c >= 0.11 libbson-1.0 >= 0.0.0 libmongoc-1.0 >= 0.0.0 libsnappy >= 1.1.3],[:],
                          [
                            echo "checking modules failed. using builtin default directories."
                            AC_SUBST([OPTDIR_CPPFLAGS],["-I/opt/logjam/include -I/opt/logjam/include/libbson-1.0 -I/opt/logjam/include/libmongoc-1.0 -I/usr/local/include -I/usr/local/include/libbson-1.0 -I/usr/local/include/libmongoc-1.0 -I/opt/local/include -I/opt/local/include/libbson-1.0 -I/opt/local/include/libmongoc-1.0"])

                            OPTDIR_LDFLAGS=""
                            AS_IF([test -d /opt/logjam/lib], [OPTDIR_LDFLAGS="$OPTDIR_LDFLAGS -L/opt/logjam/lib"])
                            AS_IF([test -d /usr/local/lib],  [OPTDIR_LDFLAGS="$OPTDIR_LDFLAGS -L/usr/local/lib"])
                            AS_IF([test -d /opt/local/lib],  [OPTDIR_LDFLAGS="$OPTDIR_LDFLAGS -L/opt/local/lib"])
                            AC_SUBST([OPTDIR_LDFLAGS])

                            AC_SUBST([DEPS_LIBS],["-lczmq -lzmq -ljson-c -lmongoc-1.0 -lbson-1.0 -lsnappy"])]
                         )
      ])

dnl AS_IF([test "x$DEPS_LIBS" != "x"],
dnl       [ echo $DEPS_LIBS
dnl         val=`echo $DEPS_LIBS | sed 's|-L\(/[[^ ]*]\)| -L\1 -Wl,-rpath -Wl,\1|g'`
dnl         echo $val
dnl         AC_SUBST([DEPS_LIBS], $val)
dnl       ])

AS_IF([test `uname -s` == "Linux"], AC_SUBST([OPT_PTHREAD_FLAGS], ["-pthread"]), AC_SUBST([OPT_PTHREAD_FLAGS]))

AS_IF([test "x$CPUPROFILE" == "x"], AC_SUBST([OPT_PROFILER_FLAGS]), AC_SUBST([OPT_PROFILER_FLAGS], ["-lprofiler"]))

AC_CHECK_DECLS([htonll, ntohll])

AC_CHECK_FUNC([pthread_setname_np],
              [AC_DEFINE([HAVE_PTHREAD_SETNAME_NP], [1], [Have pthread_set_name_np])],
              [AC_CHECK_LIB(pthread, pthread_setname_np,
                            [AC_DEFINE([HAVE_PTHREAD_SETNAME_NP], [1], [Have pthread_set_name_np])],
                            [AC_DEFINE([HAVE_PTHREAD_SETNAME_NP], [0], [Don't have pthread_set_name_np])])])

AX_CHECK_ZLIB

AC_OUTPUT
