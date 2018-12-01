## Hardcore Test to brick your MacBook

```logjam-device <device-port>```

## Testing on OS X

First, build the test programs:

```
make check
```

In order to run the tests on OS X you will need to increase two kernel paramters: number
of total files per system and number of open files per process. The easiest way to do this
is to create the file /etc/sysctl.conf and add the following lines and reboot.

```
kern.maxfiles=40960
kern.maxfilesperproc=30000
```

Alternativly, you can also set the parameters from a root shell:

```
sysctl -w kern.maxfiles=40960
sysctl -w kern.maxfilesperproc=30000
```

In addition, you must set file limits in the shell before starting the tester and logjam-device

```ulimit -n 30000```

Or run the tasks

```
make run
make test
```

in two separate shells.
