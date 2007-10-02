#!/usr/bin/ruby

BASEDIR = "."
puts BASEDIR
FAILURE_DATA_DIR = "#{BASEDIR}/fd"

=begin
for failureRate in [5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
    for numNodes in [10, 50, 100]
        puts "************************************************************"
        puts "-----> #{numNodes}"

        system("java edu.cmu.neuron2.FailureDataGen #{failureRate} #{numNodes} 60 > #{FAILURE_DATA_DIR}/failure_data_#{failureRate}_#{numNodes}.dat")
    end
end
=end

if ARGV.size != 3
    puts "Usage: ./gen_failure_data.rb failureRate numNodes run#"
    exit
end

failureRate = ARGV[0]
numNodes = ARGV[1]
run = ARGV[2]

#puts "----->  failureRate = #{failureRate}%, numNodes = #{numNodes}, run = #{run}"
system("mkdir -p #{FAILURE_DATA_DIR}/f_#{failureRate}/n_#{numNodes}/r_#{run}");
system("java edu.cmu.neuron2.FailureDataGen #{failureRate} #{numNodes} 60 > #{FAILURE_DATA_DIR}/f_#{failureRate}/n_#{numNodes}/r_#{run}/failure_data.dat")
