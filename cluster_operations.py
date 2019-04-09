import math
import random

from fastdtw import fastdtw
from scipy.spatial.distance import euclidean

# grouping -> subsequence
# clustering -> clustered subsequence

def randomize(arr):
    """
    Apply the randomize in place algorithm to the given array
    Adopted from https://www.geeksforgeeks.org/shuffle-a-given-array-using-fisher-yates-shuffle-algorithm/
    :param array arr: the arr to randomly permute
    :return: the random;uy permuted array
    """
    for i in range(len(arr) - 1, 0, -1):
        # Pick a random index from 0 to i
        j = random.randint(0, i + 1)

        # Swap arr[i] with the element at random index
        arr[i], arr[j] = arr[j], arr[i]
    return arr


def clusterer(groups, st):
    """
    construct similarity clusters

    TODO give the option of only creating similarity clusters of given length or length range
    :param dict groups: [key = length, value = array of sebsequences of the length]
        For example:
        [[1,4,2],[6,1,4],[1,2,3],[3,2,1]] is a valid 'subsequences'
    :param float st: similarity threshold
    :return: dict clusters: [key = representatives, value = similarity cluster: array of sebsequences]
    """
    clusters = []
    for group_len in groups.keys():

        processing_groups = groups[group_len]
        processing_groups = randomize(
            processing_groups)  # randomize the sequence in the group to remove data-related bias

        for sequence in processing_groups:  # the subsequence that is going to form or be put in a similarity clustyer
            if not clusters.keys():  # if there is no item in the similarity clusters
                clusters[sequence] = [sequence]  # put the first sequence as the representative of the first cluster
            else:
                minSim = math.inf
                minRprst = None  # TODO MinRprst should not be None, catch None exception!

                for rprst in clusters.keys():  # iterate though all the similarity groups, rprst = representative
                    dist = sim_between_seq(sequence, rprst)
                    if dist < minSim:
                        minSim = dist
                        minRprst = rprst

                if minSim <= math.sqrt(group_len) * st / 2:  # if the calculated min similarity is smaller than the
                    # similarity threshold, put subsequence in the similarity cluster keyed by the min representative
                    clusters[minRprst].append(sequence)
                else:  # if the minSim is greater than the similarity threshold, we create a new similarity group
                    # with this sequence being its representative
                    if sequence in clusters.keys():
                        raise Exception('cluster_operations: clusterer: Trying to create new similarity cluster due '
                                        'to exceeding similarity threshold, target sequence is already a '
                                        'representative(key) in clusters. The sequence isz: ' + str(sequence))
                    clusters[sequence] = [sequence]


def sim_between_seq(seq1, seq2):
    """
    calculate the similarity between sequence 1 and sequence 2 using DTW

    TODO customizable distance type using Scipy
    :param seq1:
    :param seq2:
    :return float: return the similarity between sequence 1 and sequence 2
    """
    return fastdtw(seq1, seq2, dist=euclidean)


