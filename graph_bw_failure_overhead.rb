#!/usr/bin/env ruby
# vim:et:sw=4:ts=4
#
# XXX LOG PROCESSING BROKEN, SEE GRAPH.RB

BASEDIR = "."
puts BASEDIR
DATA_DIR = "#{BASEDIR}/data/failure"
GRAPH_DIR = "#{BASEDIR}/graphs"
TMP_DIR = "#{BASEDIR}/tmp"

numRuns = 2
schemes = ["simple", "sqrt", "sqrt_special", "sqrt_nofailover", "sqrt_rc_failover"]


# 2*5*2*5 = 100 runs (200 mins)
puts "************************************************************"
for numNodes in [10, 50]
    for runtype in schemes
        out = File.new("#{DATA_DIR}/#{numNodes}_#{runtype}.dat", "w")
            for failureRate in [5, 10, 15, 20, 25]
                num_avg = 0.0
                for run in 1..#{numRuns}
                    puts "-----> runtype = #{runtype}, numNode = #{numNodes}, failureRate = #{failureRate}%, run# = #{run}"
                    sub_dir = "#{runtype}/n#{numNodes}/f#{failureRate}/r#{run}"

                    # TODO :: use # of routing rounds to calculate bandwidth
                    num_avg += `for file in $(ls #{DATA_DUMP_DIR}/#{sub_dir}/*); do grep -i \"sent (measurements|recs)\" $file | tail -n1; done | awk '{print $14}' | ~/tools/UnixStat/bin/stats mean`
                end
                num_avg /= numRuns
                out.puts "#{failureRate} #{num_avg}"
        end
        out.close
    end
end


for numNodes in [10, 50]
    for gtype in ["monochrome", "color"]
        plots = []
        for scheme in schemes
            plots << "'#{DATA_DIR}/#{numNodes}_#{scheme}.dat' using 1:2 with linespoints title '#{scheme}'"
        end
        plots = plots.join(', ')

        # set output '#{graphdir}/bandwidth_#{gtype}.eps'
        File.open("bandwidth.gnuplot", "w") do |out|
            out.puts %Q{
set terminal postscript eps #{gtype}
set size 0.65
set output '#{GRAPH_DIR}/bandwidth_vs_failures_#{numNodes}nodes_#{gtype}.eps'
set title "Comparison of routing bandwidth overhead"
set xlabel "failure percentage"
set ylabel "routing bandwidth overhead (Kbps)"
set key left top
plot #{plots}
}
        end
        system("cat bandwidth.gnuplot | gnuplot -")
        system("rm bandwidth.gnuplot")
    end
end

system("rm #{DATA_DIR}/*.dat")

