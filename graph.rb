#!/usr/bin/env ruby
# vim:et:sw=2:ts=2

require 'fileutils'

basedir = "."
datadir = "#{basedir}/data"
graphdir = "#{basedir}/graphs"

case ARGV[0]
when "failures"
  numNodesRange = [10, 50]
  failureRange = [1, 2, 3, 4]
  schemes = ["simple", "sqrt"]
  numRuns = 1
when "none"
  numNodesRange = [10, 20, 30, 40]
  failureRange = [0]
  schemes = ["simple", "sqrt"]
  numRuns = 2
end

FileUtils.mkdir_p(graphdir)

for scheme in schemes
  File.open("#{datadir}/#{scheme}.dat", "w") do |out|
    for numNodes in numNodesRange
      for numFailures in failureRange
        for run in 0..numRuns
          subdir = "#{datadir}/#{scheme}/#{numNodes}/#{numFailures}"
          puts subdir
          xs = []
          for path in Dir["#{subdir}/*"].delete_if{|x| x[-1] == '0'[0]}
            if path != '0' and File.file? path
              File.open(path) do |f|
                begin
                  startTime = Integer(f.grep(/server started/)[0].split(' ', 2)[0])
                  f.seek(0)
                  bytes, endTime = 0, 0
                  f.grep(/send\.(Measurements|RoutingRecs)/) do |line|
                    fields = line.split(' ')
                    bytes += Integer(fields[-1])
                    endTime = Integer(fields[0])
                  end
                  xs << (bytes.to_f * 8/1000) / ((endTime - startTime) / 1000.0)
                rescue
                end
              end
            end
          end
        end
        averageSize = xs.inject{|x,y| x+y} / xs.size.to_f
        out.puts "#{numNodes} #{averageSize}"
      end
    end
  end
end

for gtype in ["monochrome", "color"]
    plots = []
    for scheme in schemes
        plots << "'#{datadir}/#{scheme}.dat' using 1:2 with linespoints title '#{scheme}'"
    end
    plots = plots.join(', ')

    # XXX set terminal postscript eps #{gtype}
    # set output '#{graphdir}/bandwidth_#{gtype}.eps'
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
    system("cat bandwidth.gnuplot | gnuplot -")
    system("rm bandwidth.gnuplot")
end

