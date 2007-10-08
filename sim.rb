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
  schemes = ["simple", "sqrt"]
  num_nodes_range = [10, 50]
  failure_rate_range = [25, 50, 75]
  num_runs = 2
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
            sys("java -cp scaleron.jar edu.cmu.neuron2.FailureDataGen" +
                " #{failure_rate} #{num_nodes} 60 > #{subdir}/failures.dat")
            simdata = "-DsimData=#{subdir}/failures.dat"
          else
            simdata = ''
          end

          # run for all configs with the same failure data set.
          sys("./run.bash -DtotalTime=200 -DconsoleLogFilter=all" +
              " -DnumNodes=#{num_nodes} -Dscheme=#{scheme}" +
              " -DprobePeriod=10 -DneighborBroadcastPeriod=60" +
              " #{simdata}" +
              " -DlogFileBase=#{subdir}/ > /dev/null")
        end
      end
    end
  end

when 'agg'
  for scheme in schemes
    for num_nodes in num_nodes_range

      xs = []

      for failure_rate in failure_rate_range

        if subject == 'failures'
          xs = []
        end

        for run in 1..num_runs
          subdir = eval get_subdir
          puts subdir

          for path in Dir["#{subdir}/*"].delete_if{|x| x[-1] == '0'[0]}
            puts path
            if File.file? path
              File.open(path) do |f|
                begin
                  startTime = Integer(f.grep(/server started/)[0].split(' ', 2)[0])
                  f.seek(0)
                  bytes, endTime = 0, 0
                  f.grep(/sent (measurements|recs)/) do |line|
                    fields = line.split(' ')
                    endTime = Integer(fields[0])
                    fields = line.split(', ')
                    bytes += fields[1].split(' ')[0].to_i
                  end
                  xs << (bytes.to_f * 8/1000) / ((endTime - startTime) / 1000.0)
                rescue
                end
              end
            end
          end

        end # run

        if subject == 'nofailures'
          xs.to_i
        end

      end # failure_rate

      # aggregate and append to file
      avg_size = xs.inject{|x,y| x+y} / xs.size.to_f
      File.open("#{datadir}/#{scheme}.dat", 'a') do |out|
        out.puts "#{num_nodes} #{avg_size}"
      end

    end # num_nodes
  end # scheme

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
