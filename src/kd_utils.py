"""
Key Distribution Utilities
===========================

Library functions for the Week 4 key distribution lab.
These functions handle the cryptographic and information-theoretic
operations. You call them from your protocol handlers.

Sections:
    1. Binary Symmetric Channel (BSC) simulation
    2. Min-entropy computation
    3. Repetition codes
    4. LDPC error correction (syndrome-based)
    5. Privacy amplification (Toeplitz hashing)
"""

import numpy as np
import scipy.sparse


# ── 1. Binary Symmetric Channel ──────────────────────────────────────────────

def simulate_bsc(x, flip_prob, rng):
    """Simulate a Binary Symmetric Channel (BSC).

    Each bit of the input *x* is independently flipped with probability
    *flip_prob*.  This models a noisy channel where errors occur randomly.

    Args:
        x (np.ndarray): Input bit vector (array of 0s and 1s).
        flip_prob (float): Probability of flipping each bit (0 <= p <= 1).
        rng (np.random.Generator): NumPy random generator.

    Returns:
        tuple: (y, num_errors) where *y* is the noisy output and
               *num_errors* is the count of flipped bits.

    Example:
        >>> rng = np.random.default_rng(42)
        >>> x = np.array([0, 1, 1, 0, 1])
        >>> y, errors = simulate_bsc(x, 0.1, rng)
    """
    noise = (rng.random(len(x)) < flip_prob).astype(int)
    y = x ^ noise
    return y, int(noise.sum())


# ── 2. Min-Entropy ───────────────────────────────────────────────────────────

def compute_min_entropy_bsc(n, p_correct):
    """Compute the min-entropy H_min(X|E) for a BSC eavesdropper.

    When Eve observes each bit through a BSC and gets the correct value
    with probability *p_correct*, her best guessing strategy for the full
    n-bit string has success probability p_correct^n.  The min-entropy is:

        H_min(X|E) = -log2(p_correct^n) = n * (-log2(p_correct))

    Higher min-entropy means Eve knows **less** about the key.

    Note: this formula assumes p_correct >= 0.5.  At p_correct = 0.5,
    Eve's observations are purely random (maximum uncertainty, H_min = n).
    Above 0.5, Eve has a genuine advantage and H_min < n.

    Special cases:
        - p_correct = 1.0 : Eve knows everything -> H_min = 0
        - p_correct = 0.0 : Eve always gets the inverse bit -> she can flip
          all observations to recover the key exactly -> H_min = 0
        - p_correct = 0.5 : Eve's observations are useless -> H_min = n

    Args:
        n (int): Number of bits in the key.
        p_correct (float): Probability Eve gets each bit correct (must be >= 0.5).

    Returns:
        float: Min-entropy in bits.

    Raises:
        ValueError: If p_correct < 0.5.  In that case Eve can flip her
            observations to get effective p_correct = 1 - p_correct > 0.5,
            so this formula would silently overestimate H_min.  Callers
            should ensure P_E < 0.5 (i.e. pass 1 - P_E here).

    Example:
        >>> compute_min_entropy_bsc(120, 0.6)   # Eve correct 60%
        88.4...
    """
    if p_correct >= 1.0:
        return 0.0
    if p_correct <= 0.0:
        return 0.0
    if p_correct < 0.5:
        raise ValueError(
            f"p_correct={p_correct:.4f} is less than 0.5. "
            "When Eve's correct-bit probability is below 0.5, she can flip all "
            "her observations to achieve effective p_correct = 1 - p_correct > 0.5, "
            "making H_min lower than this formula computes. "
            "Check that you are passing (1 - P_E) and that P_E < 0.5."
        )
    return n * (-np.log2(p_correct))


def compute_min_entropy_after_leakage(h_min, leaked_bits):
    """Compute remaining min-entropy after information leakage.

    When Alice sends error-correction information C over a public channel,
    Eve learns C.  The remaining min-entropy is bounded by:

        H_min(X | E, C) >= H_min(X | E) - |C|

    where |C| is the number of bits of information leaked.

    Args:
        h_min (float): Original min-entropy H_min(X|E) in bits.
        leaked_bits (int): Number of bits of information leaked (|C|).

    Returns:
        float: Remaining min-entropy (clamped to 0).
    """
    return max(0.0, h_min - leaked_bits)


def compute_secure_key_length(h_min, epsilon):
    """Compute the maximum secure output key length.

    From the Leftover Hash Lemma, the maximum key length l such that the
    extracted key is epsilon-close to uniform from Eve's perspective is:

        l = floor( H_min(X|E) + 2 * log2(epsilon) )

    (Note: log2(epsilon) is negative since epsilon < 1, so this *reduces*
    the key length.)

    Args:
        h_min (float): Min-entropy H_min(X|E) in bits.
        epsilon (float): Security parameter (statistical distance bound).

    Returns:
        int: Maximum secure key length in bits (at least 1).

    Example:
        >>> compute_secure_key_length(88.4, 1e-3)
        68
    """
    length = int(np.floor(h_min + 2 * np.log2(epsilon)))
    return max(1, length)


# ── 3. Repetition Codes ──────────────────────────────────────────────────────

def repetition_encode(x, r=3):
    """Encode each bit by repeating it *r* times.

    This is the simplest error-correcting code.  Each bit is transmitted
    r times, and the receiver uses majority voting to decode.

    Args:
        x (np.ndarray): Input bit vector of length n.
        r (int): Number of repetitions (must be odd).  Default: 3.

    Returns:
        np.ndarray: Encoded bit vector of length n * r.

    Example:
        >>> repetition_encode(np.array([0, 1, 1]), r=3)
        array([0, 0, 0, 1, 1, 1, 1, 1, 1])
    """
    return np.repeat(x, r)


def repetition_decode(y, r=3):
    """Decode a repetition code using majority voting.

    Groups the received bits into blocks of *r* and takes the majority
    vote for each block.

    Args:
        y (np.ndarray): Received bit vector (length must be divisible by r).
        r (int): Number of repetitions (must be odd).  Default: 3.

    Returns:
        np.ndarray: Decoded bit vector of length len(y) // r.

    Example:
        >>> repetition_decode(np.array([0, 0, 1, 1, 1, 0, 1, 0, 1]), r=3)
        array([0, 1, 1])
    """
    n = len(y) // r
    blocks = y[: n * r].reshape(n, r)
    return (blocks.sum(axis=1) > r // 2).astype(int)


def effective_error_prob_repetition(p, r=3):
    """Compute the effective error probability after repetition decoding.

    For a BSC with flip probability *p*, after sending each bit *r* times
    and taking the majority vote, the effective flip probability is:

        p_eff = sum_{k=ceil(r/2)}^{r} C(r,k) * p^k * (1-p)^{r-k}

    This is the probability that a majority of the r copies are flipped.

    Args:
        p (float): Original BSC flip probability.
        r (int): Number of repetitions (must be odd).  Default: 3.

    Returns:
        float: Effective flip probability after majority decoding.

    Example:
        >>> round(effective_error_prob_repetition(0.05, r=3), 4)
        0.0073
        >>> round(effective_error_prob_repetition(0.4, r=3), 3)
        0.352
    """
    from scipy.special import comb

    threshold = r // 2 + 1
    return sum(
        comb(r, k, exact=True) * p**k * (1 - p) ** (r - k)
        for k in range(threshold, r + 1)
    )


# ── 4. LDPC Error Correction ─────────────────────────────────────────────────

def make_ldpc_matrix(n, d_v, d_c, seed=None):
    """Build a (d_v, d_c)-regular LDPC parity-check matrix.

    Uses Gallager's construction to create a regular LDPC code.
    The resulting matrix H has dimensions m x n where m = d_v * (n / d_c).
    Each column has exactly d_v ones; each row has exactly d_c ones.

    Args:
        n (int): Code length (number of columns).
        d_v (int): Variable node degree (ones per column).
        d_c (int): Check node degree (ones per row).  Must divide n.
        seed (int, optional): RNG seed for reproducible matrix construction.

    Returns:
        scipy.sparse.csr_matrix: The m x n parity-check matrix.

    Example:
        >>> H = make_ldpc_matrix(120, 3, 10, seed=42)
        >>> H.shape
        (36, 120)
    """
    rng = np.random.default_rng(seed)
    m = n // d_c
    row_weight = n // m
    base_rows = np.repeat(np.arange(m), row_weight)
    base_cols = np.arange(n)

    all_rows, all_cols = [], []
    for i in range(d_v):
        perm = np.arange(n) if i == 0 else rng.permutation(n)
        all_rows.append(base_rows + i * m)
        all_cols.append(perm)

    r, c = np.concatenate(all_rows), np.concatenate(all_cols)
    return scipy.sparse.coo_matrix(
        (np.ones(len(r), dtype=np.uint8), (r, c)), shape=(d_v * m, n),
    ).tocsr()


def compute_syndrome(H, x):
    """Compute the syndrome s = H * x mod 2.

    The syndrome encodes which parity checks are violated by the input
    vector x.  For a valid codeword, the syndrome is all zeros.

    Args:
        H (scipy.sparse matrix): Parity-check matrix (m x n).
        x (np.ndarray): Bit vector of length n.

    Returns:
        np.ndarray: Syndrome vector of length m.
    """
    return np.asarray((H @ x) % 2).flatten()


def decode_syndrome(H, s_err, error_rate):
    """Estimate the error pattern from a syndrome using BP+OSD decoding.

    Given the "syndrome error" s_err = s_A XOR s_B (XOR of Alice's and
    Bob's syndromes), this function estimates the error pattern that
    caused the syndrome difference.  Bob can then correct his bit string
    by XOR-ing with the estimated error.

    Uses belief propagation (BP) with ordered statistics decoding (OSD)
    as a post-processing step for improved performance.

    Args:
        H (scipy.sparse matrix): Parity-check matrix (m x n).
        s_err (np.ndarray): Syndrome error vector (s_A XOR s_B).
        error_rate (float): Expected BSC error rate (for BP initialization).

    Returns:
        np.ndarray: Estimated error pattern of length n.
    """
    from ldpc import BpOsdDecoder

    decoder = BpOsdDecoder(
        H,
        error_rate=error_rate,
        max_iter=100,
        osd_method="osd_cs",
        osd_order=7,
        input_vector_type="syndrome",
    )
    return decoder.decode(np.asarray(s_err, dtype=np.uint8))


# ── 5. Privacy Amplification (Toeplitz Hashing) ─────────────────────────────

def generate_seed(input_length, output_length, rng):
    """Generate a random seed for Toeplitz hashing.

    A Toeplitz matrix is defined by its first row and first column,
    so the seed needs (input_length + output_length - 1) random bits.
    The seed is public -- even if Eve knows it, the Leftover Hash Lemma
    guarantees that the extracted key is close to uniform.

    Alice generates this seed, sends it to Bob, and both sides pass it
    into privacy_amplify_with_seed to extract the same final key.

    Args:
        input_length (int): Length of the input bit vector (n).
        output_length (int): Desired output key length (l).
        rng (np.random.Generator): NumPy random generator.

    Returns:
        np.ndarray: Random seed of the required length (uint8 array of 0s and 1s).

    Example:
        >>> rng = np.random.default_rng(42)
        >>> seed = generate_seed(120, 68, rng)
        >>> len(seed)
        187
    """
    from randextract import ToeplitzHashing

    ext = ToeplitzHashing(input_length=input_length, output_length=output_length)
    return rng.integers(0, 2, size=ext.seed_length).astype(np.uint8)


def privacy_amplify_with_seed(x, key_len, seed):
    """Apply Toeplitz hashing with a given seed.

    Alice calls generate_seed() to create a random seed, then both Alice
    and Bob call this function with the same seed to extract the same
    final key.  Even if Eve learns the seed, the Leftover Hash Lemma
    guarantees the output is statistically close to uniform.

    Args:
        x (np.ndarray): Input bit vector (will be cast to uint8).
        key_len (int): Output key length in bits.
        seed (np.ndarray): Toeplitz seed generated by generate_seed().

    Returns:
        np.ndarray: Extracted key of length key_len.
    """
    from randextract import ToeplitzHashing

    ext = ToeplitzHashing(input_length=len(x), output_length=key_len)
    return ext.extract(x.astype(np.uint8), seed)
