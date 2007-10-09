#!/usr/bin/env ruby
# vim:et:sw=2:ts=2

require 'fileutils'

basedir = "."
datadir = "#{basedir}/data"
graphdir = "#{basedir}/graphs"

mode = ARGV[0]
subject = ARGV[1]

if ARGV.size != 2
  puts '
  ./sim.rb mode subject
    mode: run, agg, plot
    subject: nofailures, failures
  '
  exit
end

case subject
when 'failures'
  schemes = ["simple", "sqrt", "sqrt_nofailover"]
  num_nodes_range = [50]
  failure_rate_range = [50, 90]
  num_runs = 1
when 'nofailures'
  schemes = ["simple", "sqrt"]
  num_nodes_range = [10, 20, 30, 40]
  failure_rate_range = [0]
  num_runs = 1
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

  end # run
end

# aggregate xs and append to out file
def append_agg(xs, out, param)
  avg_size = xs.inject{|x,y| x+y} / xs.size.to_f
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
            total_time = 120
          end

          # run for all configs with the same failure data set.
          sys("./run.bash -DtotalTime=#{total_time} -DconsoleLogFilter=all" +
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
          agg_runs(scheme, num_nodes, failure_rate, xs)
          append_agg(xs,out,num_nodes)
        end
      end
    end

  # same as above, but one file per num_nodes, and now the x-axis is not
  # num_nodes but rather the failure_rate
  when 'failures'
    for scheme in schemes
      for num_nodes in num_nodes_range
        File.open("#{datadir}/bandwidth-#{scheme}-failures-n#{num_nodes}.dat", 'a') do |out|
          for failure_rate in failure_rate_range
            xs = []
            agg_runs(scheme, num_nodes, failure_rate, xs)
            append_agg(xs,out,failure_rate)
          end
        end
      end
    end

    for scheme in schemes
      for num_nodes in num_nodes_range
        File.open("#{datadir}/availability-#{scheme}-failures-n#{num_nodes}.dat", 'a') do |out|
          for failure_rate in failure_rate_range
            xs = []

            for run in 1..num_runs
              subdir = eval get_subdir
              puts subdir

              for path in Dir["#{subdir}/*"].delete_if{|x| not is_nonzero_int_and_file(x)}
                File.open(path) do |f|
                  begin
                    start_time = Integer(f.grep(/server started/)[0].split(' ', 2)[0])
                    f.seek(0)
                    live_nodes, avg_paths, count, end_time = 0, 0, 0, 0
                    f.grep(/ live nodes, /) do |line|
                      fields = line.split(': ')
                      end_time = Integer(fields[0].split(' ')[0])
                      fields = fields[1].split(', ')
                      live_nodes += fields[0].split(' ')[0].to_i
                      avg_paths += fields[1].split(' ')[0].to_i
                      count += 1
                    end
                    xs << (bytes.to_f * 8/1000) / ((end_time - start_time) / 1000.0)
                  rescue
                  end
                end
              end

            end # run

            append_agg(xs,out,failure_rate)
          end
        end
      end
    end
  end

when 'plot'
  remkdir(graphdir)
  for gtype in ["monochrome", "color"]
    plots = []
    for scheme in schemes
      plots << "'#{datadir}/#{scheme}.dat' using 1:2 with linespoints title '#{scheme}'"
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
end
