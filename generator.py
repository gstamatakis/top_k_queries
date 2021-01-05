import argparse
import os

import matplotlib.pyplot as plt
import numpy as np

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generates the dataset.')

    parser.add_argument('-d', '--dimensions', required=True, type=int,
                        help='The number of dimensions (columns) per row.')

    parser.add_argument('-r', '--rows', required=True, type=int,
                        help='The number of entries (rows) to generate.')

    parser.add_argument('-dm', '--dim-max', required=False, type=int, default=1,
                        help='The max int value of a dimension entry.')

    parser.add_argument('-rdn', '--rand-digit-num', required=False, type=int, default=6,
                        help='The length of each number-entry (includes \'.\').')

    parser.add_argument('-o', '--output', required=False, default='datasets',
                        help='The output *FOLDER* location and name (eg. datasets).')

    parser.add_argument('-s', '--seed', required=False, default=0, type=int,
                        help='The random number generator seed.')

    parser.add_argument('--distribution', required=False, default='uniform', type=str,
                        help='The distribution of the randomly generated points (uniform,normal).')

    parser.add_argument('--mean', required=False, default='0', type=float, help='Mean of the distribution.')
    parser.add_argument('--stddev', required=False, default='1', type=float, help='stddev of the distribution.')

    parser.add_argument('--rotate90', action='store_true', dest="rotate90", help='Rotate the normal distribution.')
    parser.add_argument('--vis', action='store_true', dest='vis', help='Visualize the distribution histogram.')

    args = parser.parse_args()

    rows = args.rows
    dims = args.dimensions
    out = args.output
    dim_max = args.dim_max
    random_seed = args.seed
    digits = args.rand_digit_num
    distribution = args.distribution
    mean, stddev = args.mean, args.stddev
    rotate90 = args.rotate90
    vis = args.vis

    # Random numbers and output path
    np.random.seed(0)
    if distribution == 'uniform':
        randNums = np.random.uniform(low=0, high=dim_max, size=(rows, dims)).astype(np.float16)
        out_path = os.path.join(out, '{0}_{1}_U.csv'.format(rows, dims, distribution))
    elif distribution == 'normal':
        randNums = np.random.normal(loc=mean, scale=stddev, size=(rows, dims)).clip(0, dim_max).astype(np.float16)
        out_path = os.path.join(out, '{0}_{1}_N({2},{3}).csv'.format(rows, dims, mean, stddev))

    else:
        raise RuntimeError("Distribution can be [normal,uniform].")
    # Output path

    # Write to csv file.
    np.savetxt(out_path, randNums, delimiter=",", fmt='%.5f', newline='\n')

    # Visualize the result, compare it to another normal distribution
    binCnt = 100
    if vis:
        fig, axs = plt.subplots(2)
        fig.suptitle("".format(distribution))

        count, bins, ignored = axs[0].hist(randNums[:, [0]], binCnt, density=True, range=[0, 1])
        axs[0].set_title('{0} distribution histogram'.format(distribution))

        if distribution == 'normal':
            axs[0].plot(bins,
                        1 / (stddev * np.sqrt(2 * np.pi)) * np.exp(- (bins - mean) ** 2 / (2 * stddev ** 2)),
                        linewidth=2, color='r')

        # Scatter plot of the 1st and 2nd dimensions iff present
        axs[1].scatter(randNums[:, [0]], randNums[:, [1]], color='b')
        axs[1].set_xlabel('1st dimension')
        axs[1].set_ylabel('2nd dimension')
        axs[1].set_title('Points scatter plot')
        plt.show()
