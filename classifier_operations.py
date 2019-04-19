from fastdtw import fastdtw
from scipy.spatial.distance import euclidean


def sim_between_seq(seq1, seq2):
    """
    calculate the similarity between sequence 1 and sequence 2 using DTW

    TODO customizable distance type using Scipy
    :param seq1:
    :param seq2:
    :return float: return the similarity between sequence 1 and sequence 2
    """
    return fastdtw(seq1, seq2, dist=euclidean)[0]  # fastdtw returns a tuple with the first item being the distance
    # and the second is the shortest path