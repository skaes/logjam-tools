AC_INIT([logjam-tools], [0.1], [skaes@railsexpress.de],
        [logjam-tools], [https://github.com/skaes/logjam-device])
AC_PREREQ([2.59])
AM_INIT_AUTOMAKE([1.10 -Wall no-define foreign])

# AC_CONFIG_HEADERS([config.h])

OLD_CFLAGS=$CFLAGS
test -z "$OLD_CFLAGS" && OLD_CFLAGS="-O3 -ggdb"
OLD_CC=$CC
test -z "$OLD_CC" && test `uname -s` = "Darwin" && OLD_CC="clang"
AC_PROG_CC
test -z "$OLD_CFLAGS" || CFLAGS=$OLD_CFLAGS
test -z "$OLD_CC" || CC=$OLD_CC

AC_CONFIG_FILES([Makefile])

# PKG_CHECK_MODULES([DEPS], [libzmq >= 3.2.4 libczmq >= 2.0.2 librabbitmq >= 0.4.1])

AC_ARG_WITH(opt-dir,
	AS_HELP_STRING([--with-opt-dir=DIR-LIST],
		       [add optional headers and libraries directories separated by $PATH_SEPARATOR]),
	[
		val=`echo "$PATH_SEPARATOR$withval" | sed "s|$PATH_SEPARATOR\([[^$PATH_SEPARATOR]*]\)| -I\1/include|g;s/^ //"`
		OPTDIR_CPPFLAGS="$val"
		val=`echo "$PATH_SEPARATOR$withval" | sed "s|$PATH_SEPARATOR\([[^$PATH_SEPARATOR]*]\)| -L\1/lib|g;s/^ //"`
		OPTDIR_LDFLAGS="$val"
	])

if test -z "$OPTDIR_CPPFLAGS"; then
  OPTDIR_CPPFLAGS="-I/usr/local/include -I/opt/local/include"
  OPTDIR_LDFLAGS="-L/usr/local/lib -L/opt/local/lib"
fi
AC_SUBST([OPTDIR_CPPFLAGS])
AC_SUBST([OPTDIR_LDFLAGS])

AC_SUBST([DEPS_LIBS],["-lrabbitmq -lzmq -lczmq"])
AC_OUTPUT
