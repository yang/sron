#!/usr/bin/env ruby
# vim:et:sw=2:ts=2

basedir = "."
datadir = "#{basedir}/data"
graphdir = "#{basedir}/graphs"
schemes = ["simple", "sqrt", "sqrt_special"]

for scheme in schemes
  File.open("#{datadir}/#{scheme}.dat", "w") do |out|
    for numNodes in 5..15 # [4, 9, 16, 25, 36, 49, 64, 81, 100]
      subdir = "#{datadir}/#{scheme}/#{numNodes}"
      xs = []
      for path in Dir["#{subdir}/*"].delete_if{|x| x[-1] == '0'[0]}
        File.open(path) do |f|
          startTime = Integer(f.grep(/server started/)[0].split(' ', 2)[0])
          f.seek(0)
          bytes, endTime = 0, 0
          f.grep(/send\.(Measurements|RoutingRecs)/) do |line|
            fields = line.split(' ')
            bytes += Integer(fields[-1])
            endTime = Integer(fields[0])
          end
          xs << bytes.to_f / ((endTime - startTime) / 1000.0)
        end
      end
      averageSize = xs.inject{|x,y| x+y} / xs.size.to_f
      out.puts "#{numNodes} #{averageSize}"
    end
  end
end

plots = []
for scheme in schemes
  plots << "'#{scheme}.dat' using 1:2 with linespoints title '#{scheme}'"
end
plots = plots.join(', ')

File.open("#{datadir}/bandwidth.gnuplot", "w") do |out|
  out.puts %Q{
set terminal postscript pdf monochrome
set size 0.65
set output '../#{graphdir}/bandwidth.pdf'
set title "Comparison of routing bandwidth overhead"
set xlabel "number of nodes"
set ylabel "routing bandwidth overhead (bytes/sec)"
plot #{plots}
}
end

# system("cat bandwidth.gnuplot | gnuplot -")