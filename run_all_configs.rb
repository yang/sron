#!/usr/bin/ruby

BASEDIR = "."
puts BASEDIR
DATA_DUMP_DIR = "#{BASEDIR}/data"
TEMPDIR = "/tmp"

system("mkdir -p #{DATA_DUMP_DIR}");
system("rm #{TEMPDIR}/scaleron-log-*")

for runtype in ["simple", "sqrt", "sqrt_special"]
    puts runtype
    for numNodes in 4..100
        puts "************************************************************"
        puts "-----> #{numNodes}"
        sub_dir = "#{runtype}/#{numNodes}"
        system("mkdir -p #{DATA_DUMP_DIR}/#{sub_dir}");
        system("rm -f #{DATA_DUMP_DIR}/#{sub_dir}/*");
        system("./run.bash delay -Dlogfilter='send.Ping recv.Ping send.Pong recv.Pong' -DnumNodes=#{numNodes} -Dscheme=#{runtype}")

        system("sleep 2")
        puts ">>>>>"
        puts "moving logs from #{TEMPDIR}/scaleron-log-* to #{DATA_DUMP_DIR}/#{sub_dir}/"
        system("mv #{TEMPDIR}/scaleron-log-* #{DATA_DUMP_DIR}/#{sub_dir}/")
    end
end