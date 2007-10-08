#!/usr/bin/env ruby
# vim:et:sw=2:ts=2

BASEDIR = "."
puts BASEDIR
DATA_DUMP_DIR = "#{BASEDIR}/data/failure"

system("mkdir -p #{DATA_DUMP_DIR}");

# 2*5*2*5 = 100 runs (200 mins)
for run in 1..2
  for failureRate in [5, 10, 15, 20, 25]
    for numNodes in [10, 50]
      puts "************************************************************"
      puts "generating failure data ..."
      puts "-----> failureRate = #{failureRate}%, numNodes = #{numNodes}, run# = #{run}"

      # generate failure data
      system("./gen_failure_data.rb #{failureRate} #{numNodes} #{run}")

      # for scheme in ["simple", "sqrt", "sqrt_special", "sqrt_nofailover", "sqrt_rc_failover"]
      for scheme in ["simple", "sqrt"]
        puts "running experiment ..."
        puts "-----> scheme = #{scheme}, numNode = #{numNodes}, failureRate = #{failureRate}%, run# = #{run}"
        subdir = "#{DATA_DUMP_DIR}/#{scheme}/#{numNodes}/#{failureRate}/#{run}"
        system("mkdir -p #{subdir}");
        system("rm -f #{subdir}/*");

        # run for all configs with the same failure data set.
        system("./run.bash -DtotalTime=200 -DconsoleLogFilter=all"
               " -DnumNodes=#{numNodes} -Dscheme=#{scheme}"
               " -DprobePeriod=10 -DneighborBroadcastPeriod=30"
               " -DsimData=./fd/#{failureRate}/#{numNodes}/#{run}/failure_data.dat"
               " -DlogFileBase=#{subdir} > /dev/null")
      end
    end
  end
end
