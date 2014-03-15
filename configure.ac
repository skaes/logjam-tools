AC_INIT([logjam-tools], [0.1], [skaes@railsexpress.de],
        [logjam-tools], [https://github.com/skaes/logjam-device])
AC_PREREQ([2.59])
AM_INIT_AUTOMAKE([1.10 -Wall no-define foreign])

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
AC_PROG_CC
test -z "$OLD_CFLAGS" || CFLAGS=$OLD_CFLAGS
test -z "$OLD_CC" || CC=$OLD_CC

AC_CONFIG_FILES([Makefile])

AC_ARG_WITH(opt-dir,
	AS_HELP_STRING([--with-opt-dir=DIR-LIST],
		       [add optional headers and libraries directories separated by $PATH_SEPARATOR]),
	[
		val=`echo "$PATH_SEPARATOR$withval" | sed "s|$PATH_SEPARATOR\([[^$PATH_SEPARATOR]*]\)| -I\1/include|g;s/^ //"`
		OPTDIR_CPPFLAGS="$val"
		val=`echo "$PATH_SEPARATOR$withval" | sed "s|$PATH_SEPARATOR\([[^$PATH_SEPARATOR]*]\)| -L\1/lib|g;s/^ //"`
		OPTDIR_LDFLAGS="$val"
                AC_SUBST([OPTDIR_CPPFLAGS])
		AC_SUBST([OPTDIR_LDFLAGS])
                AC_SUBST([DEPS_LIBS],["-lrabbitmq -lzmq -lczmq -ljson-c"])
	])

AS_IF([test "x$with_opt_dir" == "x"],
      [ PKG_CHECK_MODULES([DEPS],[libzmq >= 3.2.4 libczmq >= 2.0.2 librabbitmq >= 0.4.1],[:],
                          [ AC_SUBST([OPTDIR_CPPFLAGS],["-I/usr/local/include -I/opt/local/include"])
                            AC_SUBST([OPTDIR_LDFLAGS],["-L/usr/local/lib -L/opt/local/lib"])
                            AC_SUBST([DEPS_LIBS],["-lrabbitmq -lzmq -lczmq  -ljson-c"])]
                         )
      ])

AC_OUTPUT
