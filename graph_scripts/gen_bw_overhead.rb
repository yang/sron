#!/usr/bin/ruby

BASEDIR = ".."
puts BASEDIR
DATA_DUMP_DIR = "#{BASEDIR}/data_dump/"
GRAPH_DIR = "#{BASEDIR}/graphs"
TMP_DIR = "#{BASEDIR}/tmp"

for runtype in ["SIMPLE", "SQRT", "SQRT_SPECIAL"]
    puts runtype
    out = File.new("./#{runtype}.dat", "w")
    for numNodes in 4...10
        sub_dir = "#{runtype}/#{numNodes}"

        #system("for file in $(ls #{DATA_DUMP_DIR}/#{sub_dir}/scaleron-log-*); do grep -i \"bandwidth\" $file | tail -n1; done | awk '{print $14}' | ~/tools/UnixStat/bin/stats min max mean");
        num_avg = `for file in $(ls #{DATA_DUMP_DIR}/#{sub_dir}/scaleron-log-*); do grep -i \"bandwidth\" $file | tail -n1; done | awk '{print $14}' | ~/tools/UnixStat/bin/stats mean`
        out.puts "#{numNodes} #{num_avg}"
    end
    out.close
end



out2 = File.new("graph_neuron_bw_overhead_comparison.gp", "w")

out2.puts "set terminal postscript pdf monochrome;"
out2.puts "set size 0.65"
out2.puts "set output '#{GRAPH_DIR}/_graph_neuron_bw_overhead_comparison.eps';"
out2.puts "set title \"Comparison of routing bandwidth overhead\";"
out2.puts "set xlabel \"number of nodes\"; set ylabel \"routing bandwidth overhead (bytes/sec)\";"
str = "plot "
for runtype in ["SIMPLE", "SQRT", "SQRT_SPECIAL"]
        str += "'#{runtype}.dat' using 1:2 with linespoints title '#{runtype}'"
        if runtype != "SQRT_SPECIAL" then
                str += ","
        end
end

out2.puts "#{str};"
out2.puts ""

out2.close
system("cat graph_neuron_bw_overhead_comparison.gp | gnuplot -")

system("rm graph_neuron_bw_overhead_comparison.gp")
system("rm *.dat")

