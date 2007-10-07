#!/usr/bin/env ruby
# vim:et:sw=4:ts=4

BASEDIR = "."
puts BASEDIR
FAILURE_DATA_DIR = "#{BASEDIR}/fd"

if ARGV.size != 3
    puts "Usage: ./gen_failure_data.rb failureRate numNodes run#"
    exit
end

failureRate = ARGV[0]
numNodes = ARGV[1]
run = ARGV[2]

subdir = "#{FAILURE_DATA_DIR}/#{failureRate}/#{numNodes}/#{run}";
system("mkdir -p #{subdir}");
system("java -cp scaleron.jar edu.cmu.neuron2.FailureDataGen #{failureRate} #{numNodes} 60 > #{subdir}/failure_data.dat")
