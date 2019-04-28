AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = ${ACLOCAL_FLAGS} -I m4
SUBDIRS = src

dist_bin_SCRIPTS = logjam-prometheus-config

export bindir
export prefix
export INSTALL_PROGRAM
export PKG_CONFIG_PATH
export OPTDIR_LDFLAGS

all-local:
	$(MAKE) -C go $(MFLAGS) all

clean-local:
	$(MAKE) -C go $(MFLAGS) clean

install-exec-local:
	$(MAKE) -C go $(MFLAGS) install

uninstall-local:
	$(MAKE) -C go $(MFLAGS) uninstall

check-local:
	$(MAKE) -C go $(MFLAGS) check
