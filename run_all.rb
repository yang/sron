#!/usr/bin/ruby

out = File.new("./ron.dat", "w")
out1 = File.new("./neuron.dat", "w")
out.close
out1.close

puts "RON"
for numNodes in [4, 9, 16, 25, 36, 49, 64, 81, 100]
        system("java edu.cmu.nuron.RonTest #{numNodes} localhost 8100 false >> ron.dat")
end


puts "NEURON"
for numNodes in [4, 9, 16, 25, 36, 49, 64, 81, 100]
        system("java edu.cmu.nuron.RonTest #{numNodes} localhost 8100 true >> neuron.dat")
end

out2 = File.new("./graph_ron_vs_neuron_bw_overhead.gp", "w")

# MAGNIFIED SECTIONS - DATA, ACK, SYNCH
out2.puts "set terminal postscript eps monochrome;"
out2.puts "set size 0.65"
out2.puts "set output '_graph_ron_vs_neuron_bw_overhead.eps';"
out2.puts "set title \"Comparison of routing bandwidth overhead\";"
out2.puts "set xlabel \"Number of nodes\"; set ylabel \"routing bandwidth overhead (bytes)\";"
#out2.puts "set xtics rotate by 90;"
out2.puts "plot 'ron.dat' using 1:2 with linespoints title 'Full Mesh', 'neuron.dat' using 1:2 with linespoints title 'Grid Quorum (2 rendezvous)';"
out2.puts ""

out2.close
system("cat ./graphs_ron_vs_neuron_bw_overhead.gp | gnuplot -")
