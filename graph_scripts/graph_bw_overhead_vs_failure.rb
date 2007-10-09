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

if true
puts "************************************************************"
for numNodes in [50]
    for runtype in schemes
        allout = File.new("#{DATA_DIR}/bw_#{numNodes}_#{runtype}_all.dat", "w")
        out = File.new("#{DATA_DIR}/bw_#{numNodes}_#{runtype}_avg.dat", "w")

        for failureRate in [50, 90]

            xs = []
            for run in 1..numRuns
                puts "-----> runtype = #{runtype}, numNode = #{numNodes}, failureRate = #{failureRate}%, run# = #{run}"
                sub_dir = "#{DATA_DIR}/#{runtype}/n#{numNodes}/f#{failureRate}/r#{run}"
                puts "#{sub_dir}"

                # one file per subdir (one line per node) -> "bw in Kbps"
                bw_dat = File.new("#{sub_dir}/bw.dat", "w")

                for path in Dir["#{sub_dir}/*"].delete_if{|x| not is_nonzero_int_and_file(x)}
                    File.open(path) do |f|
                        begin
                            start_time = Integer(f.grep(/server started/)[0].split(' ', 2)[0])
                            bytes, end_time = 0, 0

                            # count outgoing messages
                            f.seek(0)
                            f.grep(/sent (measurements|recs)/) do |line|
                                fields = line.split(' ')
                                end_time = [end_time, Integer(fields[0])].max()
                                fields = line.split(', ')
                                bytes += fields[1].split(' ')[0].to_i
                            end

                            # count incoming messages
                            f.seek(0)
                            regex = Regexp.new('(\d+) .*recv.(?:Measurements|RoutingRecs).*len (\d+)')
                            f.each do |line|
                                match = regex.match(line)
                                if match
                                    end_time = [end_time, Integer(match[1])].max()
                                    bytes += match[2].to_i
                                end
                            end

                            node_bw = (bytes.to_f * 8/1000) / ((end_time - start_time) / 1000.0)
                            bw_dat.puts "#{node_bw}"
                        rescue
                        end
                    end
                end
                bw_dat.close
            end #runs

            # find (min, max, average) across runs, of the averages in each run
            f_dir = "#{DATA_DIR}/#{runtype}/n#{numNodes}/f#{failureRate}"
            # "min, max, mean" of averages, across runs
            # has one line per run
            avg_bw_dat = File.new("#{f_dir}/avg_bw.dat", "w")
            for run in 1..numRuns
                stats = `cat #{f_dir}/r#{run}/bw.dat | ../../tools/UnixStat/bin/stats min max mean`
                avg_bw_dat.puts "#{stats}"
            end
            avg_bw_dat.close

            min_bw_across_runs = `cat #{f_dir}/avg_bw.dat | awk '{print $1}' | ../../tools/UnixStat/bin/stats mean`
            max_bw_across_runs = `cat #{f_dir}/avg_bw.dat | awk '{print $2}' | ../../tools/UnixStat/bin/stats mean`
            avg_bw_across_runs = `cat #{f_dir}/avg_bw.dat | awk '{print $3}' | ../../tools/UnixStat/bin/stats mean`
            puts "#{failureRate} #{min_bw_across_runs.chomp} #{avg_bw_across_runs.chomp} #{max_bw_across_runs.chomp}"
            allout.puts "#{failureRate} #{min_bw_across_runs.chomp} #{avg_bw_across_runs.chomp} #{max_bw_across_runs.chomp}"

            puts "#{failureRate} #{avg_bw_across_runs.chomp}"
            out.puts "#{failureRate} #{avg_bw_across_runs.chomp}"

        end
        out.close
        allout.close
    end
end
end

for numNodes in [50]
    for gtype in ["monochrome", "color"]
        plots = []
        for scheme in schemes
            plots << "'#{DATA_DIR}/bw_#{numNodes}_#{scheme}_all.dat' with yerrorbars title '#{scheme}'"
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
