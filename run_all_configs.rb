#!/usr/bin/ruby

BASEDIR = "."
puts BASEDIR
DATA_DUMP_DIR = "#{BASEDIR}/data_dump"
TEMPDIR = "/tmp"

system("mkdir -p #{DATA_DUMP_DIR}");
system("rm #{TEMPDIR}/scaleron-log-*")

for runtype in ["simple.neuron", "neuron", "sq_root_special.neuron"]
    puts runtype
    for numNodes in 4...6
        puts "************************************************************"
        puts "-----> #{numNodes}"
        sub_dir = "#{runtype}/#{numNodes}"
        system("mkdir -p #{DATA_DUMP_DIR}/#{sub_dir}");
        system("rm -f #{DATA_DUMP_DIR}/#{sub_dir}/*");
        system("./run.bash #{runtype}.properties #{numNodes}")

        system("sleep 10")
        puts ">>>>>"
        puts "moving logs from #{TEMPDIR}/scaleron-log-* to #{DATA_DUMP_DIR}/#{sub_dir}/"
        system("mv #{TEMPDIR}/scaleron-log-* #{DATA_DUMP_DIR}/#{sub_dir}/")
    end
end
