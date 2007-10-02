#!/usr/bin/ruby

BASEDIR = "."
puts BASEDIR
DATA_DUMP_DIR = "#{BASEDIR}/data/failure"
TEMPDIR = "/tmp"

system("mkdir -p #{DATA_DUMP_DIR}");
system("rm #{TEMPDIR}/scaleron-log-*")

for run in 1..2
    for failureRate in [5, 10]
        for runtype in ["sqrt"]
            #    for runtype in ["simple", "sqrt", "sqrt_special"]
            puts runtype
            for numNodes in [10]
                puts "************************************************************"
                puts "-----> #{runtype}, numNode = #{numNodes}, failureRate = #{failureRate}%, run# = #{run}"

                system("./gen_failure_data.rb #{failureRate} #{numNodes} #{run}")


                sub_dir = "#{runtype}/n_#{numNodes}/f_#{failureRate}/#{run}"
                system("mkdir -p #{DATA_DUMP_DIR}/#{sub_dir}");
                system("rm -f #{DATA_DUMP_DIR}/#{sub_dir}/*");

                system("./run.bash delay -DfileLogFilter='send.Ping recv.Ping send.Pong recv.Pong' -DconsoleLogFilter=all -DnumNodes=#{numNodes} -Dscheme=#{runtype} -DsimData=./fd/f_#{failureRate}/n_#{numNodes}/r_#{run}/failure_data.dat")

                system("sleep 2")
                puts ">>>>>"
                puts "moving logs from #{TEMPDIR}/scaleron-log-* to #{DATA_DUMP_DIR}/#{sub_dir}/"
                system("mv #{TEMPDIR}/scaleron-log-* #{DATA_DUMP_DIR}/#{sub_dir}/")
            end
        end
    end
end
