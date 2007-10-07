#!/usr/bin/ruby

BASEDIR = "."
puts BASEDIR
DATA_DUMP_DIR = "#{BASEDIR}/data/failure"
TEMPDIR = "/tmp"

system("mkdir -p #{DATA_DUMP_DIR}");
system("rm #{TEMPDIR}/scaleron-log-*")

# 2*5*2*5 = 100 runs (200 mins)
for run in 1..2
    for failureRate in [5, 10, 15, 20, 25]
        for numNodes in [10, 50]
            puts "************************************************************"
            puts "generating failure data ..."
            puts "-----> failureRate = #{failureRate}%, numNodes = #{numNodes}, run# = #{run}"

            # generate failure data
            system("./gen_failure_data.rb #{failureRate} #{numNodes} #{run}")

            for runtype in ["simple", "sqrt", "sqrt_special", "sqrt_nofailover", "sqrt_rc_failover"]
                puts "running experiment ..."
                puts "-----> runtype = #{runtype}, numNode = #{numNodes}, failureRate = #{failureRate}%, run# = #{run}"
                sub_dir = "#{runtype}/n_#{numNodes}/f_#{failureRate}/#{run}"
                system("mkdir -p #{DATA_DUMP_DIR}/#{sub_dir}");
                system("rm -f #{DATA_DUMP_DIR}/#{sub_dir}/*");

                # run for all configs with the same failure data set.
                system("./run.bash -DtotalTime=120 -DfileLogFilter='send.Ping recv.Ping send.Pong recv.Pong' -DconsoleLogFilter=all -DnumNodes=#{numNodes} -Dscheme=#{runtype} -DprobePeriod=10 -DneighborBroadcastPeriod=30 -DsimData=./fd/f_#{failureRate}/n_#{numNodes}/r_#{run}/failure_data.dat -DlogFileBase=scaleron-log-")

                system("sleep 2")
                puts ">>>>>"
                puts "moving logs from #{TEMPDIR}/scaleron-log-* to #{DATA_DUMP_DIR}/#{sub_dir}/"
                system("mv #{TEMPDIR}/scaleron-log-* #{DATA_DUMP_DIR}/#{sub_dir}/")
            end
        end
    end
end
