require "bundler/setup"
require "ansi"

module LogSystemCommands
  def system(cmd, raise_on_error: true)
    puts ANSI.green{cmd}
    if Kernel.system cmd
      return true
    else
      raise "command failed: #{cmd}" if raise_on_error
    end
  end
end
class << self
  prepend LogSystemCommands
end

UBUNTU_VERSIONS = %w(jammy focal bionic)
PACKAGES_BUILT_FOR_USR_LOCAL = [:libs, :tools]
PREFIXES = { :opt => "/opt/logjam", :local => "/usr/local" }
SUFFIXES = { :opt => "", :local => "-usr-local" }
VERSION = File.read("VERSION.txt").chomp

APTLY_HOSTS = ENV['LOGJAM_APTLY_HOSTS'].split(",")
APTLY_USER = ENV['LOGJAM_APTLY_USER']
APTLY_PASSWORD = ENV['LOGJAM_APTLY_PASSWORD']
APTLY_CREDENTIALS = "#{APTLY_USER}:#{APTLY_PASSWORD}"

LOGJAM_PACKAGE_HOST = ENV['LOGJAM_PACKAGE_HOST'] || "railsexpress.de"

def upload(deb, distribution, host)
  puts ""
  puts "uploading #{deb} for #{distribution} to #{host}"
  system "curl -u #{APTLY_CREDENTIALS} -X POST -F 'file=@packages/#{distribution}/#{deb}' https://#{host}/api/files/#{distribution}"
end

def publish(distribution, host)
  puts ""
  puts "publishing uploaded packages for #{distribution} to #{host}"
  system <<-"CMDS"
  curl -u #{APTLY_CREDENTIALS} -X POST https://#{host}/api/repos/aptly_#{distribution}/file/#{distribution}
  curl -u #{APTLY_CREDENTIALS} -X PUT -H 'Content-Type: application/json' --data '{}' https://#{host}/api/publish/aptly_#{distribution}/#{distribution}
  CMDS
end

def download(name, deb)
  system "curl -s https://railsexpress.de/packages/ubuntu/#{name}/#{deb} --output packages/#{name}/#{deb}"
end

desc "download packages from railsexpress"
task :download do
  UBUNTU_VERSIONS.each do |name|
    system "mkdir -p packages/#{name}"
    PREFIXES.each do |location, prefix|
      suffix = SUFFIXES[location]
      deb = "logjam-tools#{suffix}_#{VERSION}_amd64.deb"
      download(name, deb)
    end
  end
end

desc "upload packages to aptly servers"
task :upload do
  UBUNTU_VERSIONS.each do |name|
    APTLY_HOSTS.each do |host|
      PREFIXES.each do |location, prefix|
        suffix = SUFFIXES[location]
        deb = "logjam-tools#{suffix}_#{VERSION}_amd64.deb"
        upload(deb, name, host)
      end
      publish(name, host)
    end
  end
end
