#!/usr/bin/python3

import sys
import os
import numpy as np

def ignore_low_diff(arr, threshold):
  """Ignore points with low diff in a NumPy array.

  Args:
    arr: A NumPy array.
    threshold: The minimum difference to ignore.

  Returns:
    A NumPy array with the low-diff points removed.
  """

  diff = np.diff(arr)
  mask = np.abs(diff) < threshold
  return arr[1:][~mask]

def normalize_size(arr):
  """Subtract starting size from all other sizes in array.
  """
  arr -= arr[0]
  return arr

def combine_files(directory):
  """Combine all lines in all files in a directory.

  Args:
    directory: The directory to combine files from.

  Returns:
    A string containing all lines in all files in the directory.
  """

  time = 0
  all_space = None
  for filename in os.listdir(directory):
    # all_space = np.append(all_space, np.loadtxt('./send-baseline/transcode latencies', "int"), axis=0)
    if all_space is None:
    #    print(directory + '/' + filename)
       all_space = np.loadtxt(directory + '/' + filename, "int")
    else:
       this_space = np.loadtxt(directory + '/' + filename, "int")
       if this_space.shape < all_space.shape:
          this_space = np.append(this_space, [0], axis=0)
       if this_space.shape > all_space.shape:
          this_space = this_space[0:len(all_space)]
    #    print(all_space)
       all_space += this_space
    if time == 0:
       time = all_space.shape[0]
    # with open(os.path.join(directory, filename), "r") as f:
    #   all_lines.extend(f.readlines())
    # print(all_space)

  return all_space, time

if len(sys.argv) < 2:
    print("Please pass in the workload to aggregate")
    exit

all_space, time = combine_files('/proj/HeARTy/ceridwen-sosp-2024-artifact/scripts/results/' + sys.argv[1] + '/space')
all_space = ignore_low_diff(all_space, 1048576)
all_space = normalize_size(all_space)

# write out all space to output directory
with open(os.path.join('/proj/HeARTy/ceridwen-sosp-2024-artifact/scripts/results/' + sys.argv[1] + '/output/total_space'), "w") as f:
    for space in all_space:
        f.write(str(space) + '\n')
print(all_space[:100])
