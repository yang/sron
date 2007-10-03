#!/usr/bin/env ruby
# vim:et:sw=2:ts=2

basedir = "."
datadir = "#{basedir}/data"
graphdir = "#{basedir}/graphs"
# schemes = ["simple", "sqrt"]
schemes = ["simple"]

for scheme in schemes
  File.open("#{datadir}/#{scheme}.dat", "w") do |out|
    # for numNodes in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    # for numNodes in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200]
    for numNodes in [3, 7]
      subdir = "#{datadir}/#{scheme}/#{numNodes}"
      puts subdir
      xs = []
      for path in Dir["#{subdir}/*"].delete_if{|x| x[-1] == '0'[0]}
        if path != '0' and File.file? path
          File.open(path) do |f|
            startTime = Integer(f.grep(/server started/)[0].split(' ', 2)[0])
            f.seek(0)
            bytes, endTime = 0, 0
            f.grep(/send\.(Measurements|RoutingRecs)/) do |line|
              fields = line.split(' ')
              bytes += Integer(fields[-1])
              endTime = Integer(fields[0])
            end
            xs << (bytes.to_f * 8/1000) / ((endTime - startTime) / 1000.0)
          end
        end
      end
      averageSize = xs.inject{|x,y| x+y} / xs.size.to_f
      out.puts "#{numNodes} #{averageSize}"
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
set terminal png
set size 0.65
set output '#{graphdir}/bandwidth_#{gtype}.png'
set title "Comparison of routing bandwidth overhead"
set xlabel "number of nodes"
set ylabel "routing bandwidth overhead (Kbps)"
set key left top
plot #{plots}
}
  end
end

system("cat bandwidth.gnuplot | gnuplot -")
system("rm bandwidth.gnuplot")
