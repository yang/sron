#!/usr/bin/env ruby
# vim:et:sw=4:ts=4
#

BASEDIR = ".."
puts BASEDIR
DATA_DIR = "#{BASEDIR}/data"
GRAPH_DIR = "#{BASEDIR}/graphs"
TMP_DIR = "#{BASEDIR}/tmp"

system("mkdir -p #{TMP_DIR}");

numRuns = 1
schemes = ["sqrt"]

def is_nonzero_int_and_file(x)
  begin
    Integer( File.basename(x) ) != 0 and File.file? x
  rescue
    false
  end
end

puts "************************************************************"
if true
for numNodes in [50]
    for runtype in schemes
        out = File.new("#{DATA_DIR}/reachable_#{numNodes}_#{runtype}.dat", "w")

        for failureRate in [50, 90]

            for run in 1..numRuns
                puts "-----> runtype = #{runtype}, numNode = #{numNodes}, failureRate = #{failureRate}%, run# = #{run}"
                sub_dir = "#{DATA_DIR}/#{runtype}/n#{numNodes}/f#{failureRate}/r#{run}"
                puts "#{sub_dir}"

                # one file per subdir (one line per node) -> "min, max, mean"
                reachable_dat = File.new("#{sub_dir}/reachable.dat", "w")
                available_dat = File.new("#{sub_dir}/available_hops.dat", "w")

                for path in Dir["#{sub_dir}/*"].delete_if{|x| not is_nonzero_int_and_file(x)}
                    File.open(path) do |f|
                        begin
                            num_avg = 0
                            # one tmp file per node for stats -> "#{live_nodes} #{num_paths} #{numNodes}"
                            tmp_file = File.new("#{TMP_DIR}/stats_n#{numNodes}_tmp.dat", "w")
                            start_time = Integer(f.grep(/server started/)[0].split(' ', 2)[0])
                            f.seek(0)
                            live_nodes, avg_paths, count, end_time = 0, 0, 0, 0
                            f.grep(/ live nodes, /) do |line|
                                fields = line.split(': ')
                                end_time = Integer(fields[0].split(' ')[0])
                                fields = fields[1].split(', ')
                                live_nodes = fields[0].split(' ')[0].to_i
                                num_paths = fields[1].split(' ')[0].to_i
                                #direct_paths = fields[2].split(' ')[0].to_i

                                count += 1
                                tmp_file.puts "#{live_nodes} #{num_paths} #{numNodes}"
                            end
                            tmp_file.close
                            stats = `cat #{TMP_DIR}/stats_n#{numNodes}_tmp.dat | awk '{print $1}' | ../..//tools/UnixStat/bin/stats min max mean`
                            stats2 = `cat #{TMP_DIR}/stats_n#{numNodes}_tmp.dat | awk '{print $2}' | ../..//tools/UnixStat/bin/stats min max mean`
                            #stats3 = `cat #{TMP_DIR}/stats_n#{numNodes}_tmp.dat | awk '{print $3}' | ../..//tools/UnixStat/bin/stats min max mean`

                            # puts "reachable => #{stats}"
                            # puts "available hops => #{stats2}"

                            reachable_dat.puts "#{stats}"
                            available_dat.puts "#{stats2}"
                        rescue
                        end
                    end
                end

                reachable_dat.close
                available_dat.close

            end #runs


            # find (min, max, average) across runs, of the averages in each run
            f_dir = "#{DATA_DIR}/#{runtype}/n#{numNodes}/f#{failureRate}"
            # "min, max, mean" of averages, across runs
            # has one line per run
            avg_reachable_dat = File.new("#{f_dir}/avg_reachable.dat", "w")
            avg_available_hops_dat = File.new("#{f_dir}/avg_available_hops.dat", "w")
            for run in 1..numRuns
                # we are interested in the mean value in each of these files
                stats = `cat #{f_dir}/r#{run}/reachable.dat | awk '{print $3}' | ../..//tools/UnixStat/bin/stats min max mean`
                avg_reachable_dat.puts "#{stats}"
                stats2 = `cat #{f_dir}/r#{run}/available_hops.dat | awk '{print $3}' | ../..//tools/UnixStat/bin/stats min max mean`
                avg_available_hops_dat.puts "#{stats2}"
            end
            avg_reachable_dat.close
            avg_available_hops_dat.close

            # 
            avg_reachability_across_runs = `cat #{f_dir}/avg_reachable.dat | awk '{print $3}' | ../..//tools/UnixStat/bin/stats mean`
            avg_available_hops_across_runs = `cat #{f_dir}/avg_available_hops.dat | awk '{print $3}' | ../..//tools/UnixStat/bin/stats mean`

            puts "#{failureRate} #{avg_reachability_across_runs.chomp} #{avg_available_hops_across_runs.chomp}"
            out.puts "#{failureRate} #{avg_reachability_across_runs.chomp} #{avg_available_hops_across_runs.chomp}"
            
        end
        out.close
    end
end
end

for numNodes in [50]
    for gtype in ["monochrome", "color"]
        plots = []
        for scheme in schemes
            plots << "'#{DATA_DIR}/reachable_#{numNodes}_#{scheme}.dat' using 1:2 with linespoints title '#{scheme}'"
        end
        plots = plots.join(', ')

        # set output '#{graphdir}/bandwidth_#{gtype}.eps'
        File.open("bandwidth.gnuplot", "w") do |out|
            out.puts %Q{
set terminal postscript eps #{gtype}
set size 0.65
set output '#{GRAPH_DIR}/reachability_vs_failures_#{numNodes}nodes_#{gtype}.eps'
set title "Reachability"
set xlabel "failure percentage"
set ylabel "average # of reachable nodes per node"
set yrange [0:]
set key left bottom
plot #{plots}
}
        end
        system("cat bandwidth.gnuplot | gnuplot -")
        system("rm bandwidth.gnuplot")
    end
end

system("rm #{DATA_DIR}/*.dat")
system("rm #{TMP_DIR}/*.dat")
