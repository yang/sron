#!/usr/bin/ruby

BASEDIR = "."
puts BASEDIR
DATA_DUMP_DIR = "#{BASEDIR}/data"
TEMPDIR = "./tmp"

system("mkdir -p #{DATA_DUMP_DIR}");
system("mkdir -p #{TEMPDIR}");
system("rm #{TEMPDIR}/scaleron-log-*")

#for runtype in ["simple", "sqrt", "sqrt_special"]
for runtype in ["simple"]
    puts runtype
    for numNodes in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200]
    #for numNodes in 4..100
        puts "************************************************************"
        puts "-----> #{numNodes}"
        sub_dir = "#{runtype}/#{numNodes}"
        system("mkdir -p #{DATA_DUMP_DIR}/#{sub_dir}");
        system("rm -f #{DATA_DUMP_DIR}/#{sub_dir}/*");
        system("./run.bash -DtotalTime=120 -DfileLogFilter='send.Ping recv.Ping send.Pong recv.Pong' -DconsoleLogFilter=all -DnumNodes=#{numNodes} -Dscheme=#{runtype} -DprobePeriod=10 -DneighborBroadcastPeriod=30 -DlogFileBase=#{TEMPDIR}/scaleron-log-")

        system("sleep 2")
        puts ">>>>>"
        puts "moving logs from #{TEMPDIR}/scaleron-log-* to #{DATA_DUMP_DIR}/#{sub_dir}/"
        system("mv #{TEMPDIR}/scaleron-log-* #{DATA_DUMP_DIR}/#{sub_dir}/")
    end
end

