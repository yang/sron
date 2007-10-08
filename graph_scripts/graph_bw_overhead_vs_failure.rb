#!/usr/bin/env ruby
# vim:et:sw=4:ts=4
#

BASEDIR = ".."
puts BASEDIR
DATA_DIR = "#{BASEDIR}/data"
GRAPH_DIR = "#{BASEDIR}/graphs"
TMP_DIR = "#{BASEDIR}/tmp"

numRuns = 2
schemes = ["simple", "sqrt", "sqrt_nofailover"]

def is_nonzero_int_and_file(x)
  begin
    Integer( File.basename(x) ) != 0 and File.file? x
  rescue
    false
  end
end

puts "************************************************************"
for numNodes in [10, 50, 100]
    for runtype in schemes
        out = File.new("#{DATA_DIR}/#{numNodes}_#{runtype}.dat", "w")
        for failureRate in [5, 10, 25, 50, 75]

            xs = []
            for run in 1..numRuns
                puts "-----> runtype = #{runtype}, numNode = #{numNodes}, failureRate = #{failureRate}%, run# = #{run}"
                sub_dir = "#{runtype}/n#{numNodes}/f#{failureRate}/r#{run}"
                puts "#{sub_dir}"

                for path in Dir["#{DATA_DIR}/#{sub_dir}/*"].delete_if{|x| not is_nonzero_int_and_file(x)}
                    File.open(path) do |f|
                        begin
                            start_time = Integer(f.grep(/server started/)[0].split(' ', 2)[0])
                            f.seek(0)
                            bytes, end_time = 0, 0
                            f.grep(/sent (measurements|recs)/) do |line|
                                fields = line.split(' ')
                                end_time = Integer(fields[0])
                                fields = line.split(', ')
                                bytes += fields[1].split(' ')[0].to_i
                            end
                            xs << (bytes.to_f * 8/1000) / ((end_time - start_time) / 1000.0)
                        rescue
                        end
                    end
                end

            end #runs

            #puts xs.inject{|x,y| x+y}
            averageSize = xs.inject{|x,y| x+y} / xs.size.to_f
            out.puts "#{failureRate} #{averageSize}"
        end
        out.close
    end
end

for numNodes in [10, 50, 100]
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

