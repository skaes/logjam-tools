prefix = ENV['LOGJAM_PREFIX'] || "/opt/logjam"
suffix = prefix == "/opt/logjam" ? "" : prefix.gsub('/', '-')

name "logjam-tools#{suffix}"

full_version = File.read("#{File.expand_path(__dir__)}/VERSION.txt").chomp
f_v, f_i = full_version.split('-', 2)

version f_v
iteration f_i

vendor "skaes@railsexpress.de"

# plugin "exclude"
# exclude "#{prefix}/share/man"
# exclude "#{prefix}/share/doc"
# exclude "/usr/share/doc"
# exclude "/usr/share/man"

files "#{prefix}/bin/logjam-*"

depends "logjam-libs#{suffix}", ">= 0.10-1"

case codename
when "bionic"
  depends "libicu60"
when "focal"
  depends "libicu66"
when "jammy"
  depends "libicu70"
end

apt_setup "apt-get update -y && apt-get install apt-transport-https ca-certificates -y"
apt_setup "echo 'deb [trusted=yes] https://railsexpress.de/packages/ubuntu/#{codename} ./' >> /etc/apt/sources.list"

keep_modified_files!

run "/bin/bash", "-c", "touch -h #{prefix}/bin/logjam*"

after_install <<-"EOS"
#!/bin/bash
ldconfig
EOS
