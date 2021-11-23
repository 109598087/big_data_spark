from datasketch import MinHash, MinHashLSH
from joblib.numpy_pickle_utils import xrange
from nltk import ngrams

data = ['minhash is a probabilistic data structure for estimating the similarity between datasets',
        'finhash dis fa frobabilistic fata ftructure for festimating the fimilarity fetween fatasets',
        'weights controls the relative importance between minizing false positive',
        'wfights cfntrols the rflative ifportance befween minizing fflse posftive',
        ]

# Create an MinHashLSH index optimized for Jaccard threshold 0.5,
# that accepts MinHash objects with 128 permutations functions
lsh = MinHashLSH(threshold=0.4, num_perm=128)

# Create MinHash objects
minhashes = {}
for c, i in enumerate(data):
    minhash = MinHash(num_perm=128)
    for d in ngrams(i, 3):
        minhash.update("".join(d).encode('utf-8'))
    lsh.insert(c, minhash)
    minhashes[c] = minhash

for i in xrange(len(minhashes.keys())):
    result = lsh.query(minhashes[i])
    print("Candidates with Jaccard similarity > 0.5 for input", i, ":", result)
