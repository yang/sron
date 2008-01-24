#!/usr/bin/env ruby
# vim:et:sw=2:ts=2

require 'fileutils'

debug = false

basedir = "."
datadir = if debug then "#{basedir}/data2" else "#{basedir}/data" end
graphdir = "#{basedir}/graphs"
tmpdir = '/tmp'

valid_cmds = [
  'run-nofailures',
  'run-failures',
  'agg-nofailures',
  'agg-failures-bandwidth',
  'agg-failures-reachability',
  'plot-nofailures-bandwidth',
  'plot-failures-bandwidth',
  'plot-failures-reachability',
]

if ARGV.size != 1 or not valid_cmds.index ARGV[0]
  puts "./sim.rb {#{valid_cmds.join '|'}}"
  exit 1
end

cmd = ARGV[0].split('-')
mode, subject = cmd[0], cmd[1]

case subject
when 'failures'
  failure_rate_range = [5, 25, 50, 75, 90] # [50,90]
  num_nodes_range = [50]
  num_runs = 2
  schemes = ["simple", "sqrt"]

  if debug
    failure_rate_range = [50,90]
    num_nodes_range = [50]
    num_runs = 1
    schemes = ["sqrt"]
  end
when 'nofailures'
  failure_rate_range = [0]
  num_nodes_range = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160]
  num_runs = 1
  schemes = ["simple", "sqrt"]
end

def sys(cmd)
  if not system(cmd)
    raise "command failed: #{cmd}"
  end
end

def remkdir(path)
  begin FileUtils.rm_r(path) ; rescue ; end
  FileUtils.mkdir_p(path)
end

get_subdir = '"#{datadir}/#{scheme}/n#{num_nodes}/f#{failure_rate}/r#{run}"'

sys('make')

def is_nonzero_int_and_file(x)
  begin
    Integer( File.basename(x) ) != 0 and File.file? x
  rescue
    false
  end
end

$num_runs = num_runs
$get_subdir = get_subdir
$datadir = datadir
def agg_runs(scheme, num_nodes, failure_rate, xs)
  neighborBroadcastPeriod = 30
  datadir = $datadir
  for run in 1..$num_runs
    subdir = eval $get_subdir
    puts subdir

    for path in Dir["#{subdir}/*"].delete_if{|x| not is_nonzero_int_and_file(x)}
      File.open(path) do |f|
        begin
          start_time = Integer(f.grep(/server started/)[0].split(' ', 2)[0])
          f.seek(0)
          bytes, end_time = 0, 0
          rounds = 0
          f.grep(/sent (measurements|recs)/) do |line|
            fields = line.split(' ')
            end_time = Integer(fields[0])
            fields = line.split(', ')
            bytes += fields[1].split(' ')[0].to_i
            rounds = rounds + 1
          end
          rounds = rounds / 2

          if rounds == 0
              rounds = 1
          end
          #xs << (bytes.to_f * 8/1000) / ((end_time - start_time) / 1000.0)
          #puts (rounds * neighborBroadcastPeriod)
          xs << (bytes.to_f * 8/1000) / (rounds * neighborBroadcastPeriod)
        rescue
        end
      end
    end

  end # run
end

# aggregate xs and append to out file
def append_agg(xs, out, param)
  avg_size = xs.inject{|x,y| x+y}.to_f / xs.size.to_f
  out.puts "#{param} #{avg_size}"
end

case mode
when 'run'
  for scheme in schemes
    for num_nodes in num_nodes_range
      for failure_rate in failure_rate_range
        for run in 1..num_runs
          subdir = eval get_subdir
          puts subdir
          remkdir(subdir)

          # if failures, first generate failure data, and prepare the extra
          # arg
          if failure_rate > 0
            total_time = 300
            sys("./rand.py #{failure_rate} #{num_nodes} #{total_time} > #{subdir}/failures.dat")
            extra_args = "-DsimData=#{subdir}/failures.dat"
          else
            extra_args = ''
            total_time = 300
          end

          # run for all configs with the same failure data set.
          sys("./run.bash -DenableSubpings=false -DtotalTime=#{total_time} -DconsoleLogFilter=all" +
              " -DnumNodes=#{num_nodes} -Dscheme=#{scheme}" +
              " -DprobePeriod=10 -DneighborBroadcastPeriod=30" +
              " #{extra_args}" +
              " -DlogFileBase=#{subdir}/ > /dev/null")
        end
      end
    end
  end

when 'agg'
  case subject
  when 'nofailures'
    for scheme in schemes
      File.open("#{datadir}/bandwidth-#{scheme}-nofailures.dat", 'a') do |out|
        for num_nodes in num_nodes_range
          xs = []
          agg_runs(scheme, num_nodes, 0, xs)
          append_agg(xs,out,num_nodes)
        end
      end
    end

  # same as above, but one file per num_nodes, and now the x-axis is not
  # num_nodes but rather the failure_rate
  when 'failures'
    case cmd[2]

    when 'bandwidth'

      #########################################################################
      #
      # bandwidth
      #

      #for scheme in schemes
      #  for num_nodes in num_nodes_range
      #    File.open("#{datadir}/bandwidth-#{scheme}-failures-n#{num_nodes}.dat", 'a') do |out|
      #      for failure_rate in failure_rate_range
      #        xs = []
      #        agg_runs(scheme, num_nodes, failure_rate, xs)
      #        append_agg(xs,out,failure_rate)
      #      end
      #    end
      #  end
      #end

      for num_nodes in num_nodes_range
        for scheme in schemes
          File.open("#{datadir}/bw_#{num_nodes}_#{scheme}_all.dat", "w") do |out|
            for failure_rate in failure_rate_range
              xs = []
              for run in 1..num_runs
                subdir = eval get_subdir
                puts "#{subdir}"

                # one file per subdir (one line per node) -> "bw in Kbps"
                File.open("#{subdir}/bw.dat", "w") do |bw_dat|
                  for path in Dir["#{subdir}/*"].delete_if{|x| not is_nonzero_int_and_file(x)}
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
                end
              end #runs

              # find (min, max, average) across runs, of the averages in each run
              f_dir = "#{datadir}/#{scheme}/n#{num_nodes}/f#{failure_rate}"
              # "min, max, mean" of averages, across runs
              # has one line per run
              File.open("#{f_dir}/avg_bw.dat", "w") do |avg_bw_dat|
                for run in 1..num_runs
                  stats = `cat #{f_dir}/r#{run}/bw.dat | ../tools/UnixStat/bin/stats min max mean`
                  avg_bw_dat.puts "#{stats}"
                end
              end

              min_bw_across_runs = `cat #{f_dir}/avg_bw.dat | awk '{print $1}' | ../tools/UnixStat/bin/stats mean`
              max_bw_across_runs = `cat #{f_dir}/avg_bw.dat | awk '{print $2}' | ../tools/UnixStat/bin/stats mean`
              avg_bw_across_runs = `cat #{f_dir}/avg_bw.dat | awk '{print $3}' | ../tools/UnixStat/bin/stats mean`

              outline = "#{failure_rate} #{avg_bw_across_runs.chomp} #{min_bw_across_runs.chomp} #{max_bw_across_runs.chomp}"
              puts outline
              out.puts outline
            end
          end
        end
      end

    when 'reachability'

      #########################################################################
      #
      # reachability
      #

      for num_nodes in num_nodes_range
        for scheme in schemes
          File.open("#{datadir}/reachable_#{num_nodes}_#{scheme}.dat", "w") do |out|
            for failure_rate in failure_rate_range
              for run in 1..num_runs
                subdir = eval get_subdir
                puts "#{subdir}"

                # one file per subdir (one line per node) -> "min, max, mean"
                reachable_dat = File.new("#{subdir}/reachable.dat", "w")
                available_dat = File.new("#{subdir}/available_hops.dat", "w")

                for path in Dir["#{subdir}/*"].delete_if{|x| not is_nonzero_int_and_file(x)}
                  File.open(path) do |f|
                    begin
                      num_avg = 0
                      # one tmp file per node for stats -> "#{live_nodes} #{num_paths} #{num_nodes}"
                      File.open("#{tmpdir}/stats_n#{num_nodes}_tmp.dat", "w") do |tmp_file|
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
                          tmp_file.puts "#{live_nodes} #{num_paths} #{num_nodes}"
                        end
                      end
                      stats = `cat #{tmpdir}/stats_n#{num_nodes}_tmp.dat | awk '{print $1}' | ..//tools/UnixStat/bin/stats min max mean`
                      stats2 = `cat #{tmpdir}/stats_n#{num_nodes}_tmp.dat | awk '{print $2}' | ..//tools/UnixStat/bin/stats min max mean`
                      #stats3 = `cat #{tmpdir}/stats_n#{num_nodes}_tmp.dat | awk '{print $3}' | ..//tools/UnixStat/bin/stats min max mean`

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
              f_dir = File.dirname subdir # "#{datadir}/#{scheme}/n#{num_nodes}/f#{failure_rate}"
              # "min, max, mean" of averages, across runs
              # has one line per run
              avg_reachable_dat = File.new("#{f_dir}/avg_reachable.dat", "w")
              avg_available_hops_dat = File.new("#{f_dir}/avg_available_hops.dat", "w")
              for run in 1..num_runs
                # we are interested in the mean value in each of these files
                stats = `cat #{f_dir}/r#{run}/reachable.dat | awk '{print $3}' | ..//tools/UnixStat/bin/stats min max mean`
                avg_reachable_dat.puts "#{stats}"
                stats2 = `cat #{f_dir}/r#{run}/available_hops.dat | awk '{print $3}' | ..//tools/UnixStat/bin/stats min max mean`
                avg_available_hops_dat.puts "#{stats2}"
              end
              avg_reachable_dat.close
              avg_available_hops_dat.close

              avg_reachability_across_runs = `cat #{f_dir}/avg_reachable.dat | awk '{print $3}' | ..//tools/UnixStat/bin/stats mean`
              avg_available_hops_across_runs = `cat #{f_dir}/avg_available_hops.dat | awk '{print $3}' | ..//tools/UnixStat/bin/stats mean`

              puts "#{failure_rate} #{avg_reachability_across_runs.chomp} #{avg_available_hops_across_runs.chomp}"
              out.puts "#{failure_rate} #{avg_reachability_across_runs.chomp} #{avg_available_hops_across_runs.chomp}"

            end
          end
        end
      end

#      for scheme in schemes
#        for num_nodes in num_nodes_range
#          File.open("#{datadir}/availability-#{scheme}-failures-n#{num_nodes}.dat", 'a') do |out|
#            for failure_rate in failure_rate_range
#              xs = []
#
#              for run in 1..num_runs
#                subdir = eval get_subdir
#                puts subdir
#
#                for path in Dir["#{subdir}/*"].delete_if{|x| not is_nonzero_int_and_file(x)}
#                  File.open(path) do |f|
#                    begin
#                      start_time = Integer(f.grep(/server started/)[0].split(' ', 2)[0])
#                      f.seek(0)
#                      live_nodes, avg_paths, count, end_time = 0, 0, 0, 0
#                      f.grep(/ live nodes, /) do |line|
#                        fields = line.split(': ')
#                        end_time = Integer(fields[0].split(' ')[0])
#                        fields = fields[1].split(', ')
#                        live_nodes += fields[0].split(' ')[0].to_i
#                        avg_paths += fields[1].split(' ')[0].to_i
#                        count += 1
#                      end
#                      xs << (bytes.to_f * 8/1000) / ((end_time - start_time) / 1000.0)
#                    rescue
#                    end
#                  end
#                end
#
#              end # run
#
#              append_agg(xs,out,failure_rate)
#            end
#          end
#        end
#      end
    end # case cmd[2]

  end # case subject

#
# plotting
#

when 'plot'
  case subject
  when 'nofailures'
    remkdir(graphdir)
    for gtype in ["monochrome", "color"]
      plots = []
      for scheme in schemes
        plots << "'#{datadir}/bandwidth-#{scheme}-nofailures.dat' using 1:2 with linespoints title '#{scheme}'"
      end
      plots = plots.join(', ')

      File.open("bandwidth.gnuplot", "w") do |out|
        out.puts %Q{
        set terminal postscript eps #{gtype}
        set size 0.65
        set output '#{graphdir}/bandwidth_#{gtype}.eps'
        set title "Comparison of routing bandwidth overhead"
        set xlabel "number of nodes"
        set ylabel "routing bandwidth overhead (Kbps)"
        set key left top
        plot #{plots}
      }
      end
      sys("cat bandwidth.gnuplot | gnuplot -")
      sys("rm bandwidth.gnuplot")
    end

  when 'failures'

    case cmd[2]
    when 'bandwidth'
      for num_nodes in [50]
          for gtype in ["monochrome", "color"]
          plots = []
          for scheme in schemes
            puts "'#{datadir}/bw_#{num_nodes}_#{scheme}_all.dat' with yerrorbars title '#{scheme}'"
            plots << "'#{datadir}/bw_#{num_nodes}_#{scheme}_all.dat' with yerrorbars title '#{scheme}'"
          end
          plots = plots.join(', ')

          # set output '#{graphdir}/bandwidth_#{gtype}.eps'
          File.open("bandwidth.gnuplot", "w") do |out|
            out.puts %Q{
    set terminal postscript eps #{gtype}
    set size 0.65
    set output '#{graphdir}/bandwidth_vs_failures_#{num_nodes}nodes_#{gtype}.eps'
    set title "Comparison of routing bandwidth overhead (min, mean, max)"
    set xlabel "failure percentage"
    set ylabel "routing bandwidth overhead (Kbps)"
    set xrange [0:100]
    set yrange [0:]
    set key left bottom
    plot #{plots}
    }
          end
          sys("cat bandwidth.gnuplot | gnuplot -")
          sys("rm bandwidth.gnuplot")
        end
      end

    when 'reachability'
      for num_nodes in num_nodes_range
        for gtype in ["monochrome", "color"]
          plots = []
          for scheme in schemes
            for metric, pos in [['reachable nodes',2],['mean available hops',3]]
              plots << "'#{datadir}/reachable_#{num_nodes}_#{scheme}.dat' using 1:#{pos} with linespoints title '#{scheme} - #{metric}'"
            end
          end
          plots = plots.join(', ')

          # set output '#{graphdir}/bandwidth_#{gtype}.eps'
          File.open("bandwidth.gnuplot", "w") do |out|
            out.puts %Q{
          set terminal postscript eps #{gtype}
          set size 0.65
          set output '#{graphdir}/reachability_vs_failures_#{num_nodes}nodes_#{gtype}.eps'
          set title "Reachability"
          set xlabel "failure percentage"
          set ylabel "average # of reachable nodes per node"
          set xrange [0:100]
          set yrange [0:]
          set key left bottom
          plot #{plots}
          }
          end
          sys("cat bandwidth.gnuplot | gnuplot -")
          sys("rm bandwidth.gnuplot")
        end
      end
      #sys("rm #{datadir}/*.dat")
      #sys("rm #{tmpdir}/*.dat")

    end
  end
end
