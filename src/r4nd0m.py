from binary_frame     import BinaryFrame
from csv              import reader            as csv_reader
from data_downloader  import Argument
from data_downloader  import QuandlInterface
from generators       import Generators
from numpy            import array             as np_array
from numpy            import empty             as np_empty
from numpy            import mean              as np_mean
from os               import environ           as os_environ
from os               import path              as os_path
from os               import remove            as os_remove
from pandas           import DataFrame         as pd_DataFrame
from pandas           import read_csv          as pd_read_csv
from randomness_tests import RandomnessTester


def clean_up():
    """
    Removes the Quandl authentication
    token pickle from the system.
    return: none
    """
    try:
        os_remove("authtoken.p")
    except FileNotFoundError:
        pass


def construct_binary_frame(data_sets,
                           method,
                           token,
                           start,
                           end,
                           years_per_block,
                           isamples):
    """
    This method is used to construct a BinaryFrame object
    from a metadata file which specifies what data sets we want
    to download and what columns we are interested in from that data.
    data_sets: the file containing the data sets we want
    method: the method of conversion to binary
    token: a Quandl authentication token
    start: the start date
    end: the end date
    years_per_block: the time frame / dimension we want to look at
    return: a BinaryFrame object which can work with the RandomnessTester class
    """
    downloader   = QuandlInterface(token)
    data_file    = pd_read_csv(data_sets)
    data_sets    = list(data_file["ID"])
    drop_columns = list(data_file["DROP"])
    data_prefix  = ""
    transform    = "rdiff"
    start_date   = str(start) + "-01-01"
    end_date     = str(end) + "-01-01"
    my_arguments = []
    for i in range(len(data_sets)):
        drop = drop_columns[i].split('#')
        if drop == "":
            drop = []
        my_arguments.append(Argument(data_sets[i], start_date, end_date, data_prefix, drop, transform))
    data_frame_full = downloader.get_data_sets(my_arguments)
    binary_frame = BinaryFrame(data_frame_full, start, end, years_per_block)
    binary_frame.convert(method, independent_samples=isamples)
    return binary_frame


def main():
    m = "discretize"
    # m = "convert basis point"
    # m = "convert floating point"
    start_year, end_year = 1950, 2015
    dirname = os_path.join(os_path.dirname(os_path.realpath("__file__")), "randomness/r4nd0m/metadata/.")
    file_name = dirname + str(start_year) + " plus.csv"
    least_random_fit = 15
    least_random_interval = 1
    for interval in range(5, 6):
        path = os_path.join("metadata", file_name)
        passed = run_experiments(path, 64, 4, m, start_year, end_year, interval)
        passed_avg = np_array(passed[2::]).mean()
        if passed_avg < least_random_fit:
            least_random_fit = passed_avg
            least_random_interval = interval
    print(least_random_interval, least_random_fit)
    clean_up()


def run_experiments(data_sets,
                    block_sizes,
                    q_sizes,
                    method,
                    start,
                    end,
                    years_per_block,
                    isamples=False):
    """
    This method runs the experiments
    which were used to write the blog post.
    data_sets: the file containing a list of data sets we want
    block_sizes: a list of block sizes
    q_sizes: a list of matrix sizes
    start: the start date
    end: the end date
    methods: the methods of conversion to binary we want to test
    years_per_block: the time frame / dimension we want to look at
    return: list of passed tests
    """
    print("\n")
    print("METHOD =", method.upper())
    length = 256 * (end - start)
    gen = Generators(length)
    prng = gen.numpy_integer()
    all_passed = []
    prng_data = pd_DataFrame(np_array(prng))
    prng_data.columns = ["Mersenne"]
    prng_binary_frame = BinaryFrame(prng_data, start, end, years_per_block)
    prng_binary_frame.convert(method, convert=False, independent_samples=isamples)
    rng_tester = RandomnessTester(prng_binary_frame, False, 00, 00)
    passed = rng_tester.run_test_suite(block_sizes, q_sizes)
    for x in passed:
        all_passed.append(x)
    nrand = np_empty(length)
    for i in range(length):
        nrand[i] = (i % 10) / 10
    nrand -= np_mean(nrand)
    nrand_data = pd_DataFrame(np_array(nrand))
    nrand_data.columns = ["Deterministic"]
    nrand_binary_frame = BinaryFrame(nrand_data, start, end, years_per_block)
    nrand_binary_frame.convert(method, convert=True, independent_samples=isamples)
    rng_tester = RandomnessTester(nrand_binary_frame, False, 00, 00)
    passed = rng_tester.run_test_suite(block_sizes, q_sizes)
    for x in passed:
        all_passed.append(x)
    t = setup_environment()
    my_binary_frame = construct_binary_frame(data_sets, method, t, start, end, years_per_block, isamples)
    rng_tester = RandomnessTester(my_binary_frame, True, start, end)
    passed = rng_tester.run_test_suite(block_sizes, q_sizes)
    for x in passed:
        all_passed.append(x)
    print("\n")
    return all_passed


def setup_environment():
    """
    This method sets up your environment to run the program.
    It handles HTTP and HTTPS proxies and the Quandl
    authentication token.
    This information is read from a
    private.csv file in the metadata folder.
    return: the authentication token from Quandl
    """
    token = ""
    try:
        with open(os_path.join("metadata", ".private.csv"), "r") as csvfile:
            reader = csv_reader(csvfile, delimiter=',', quotechar='"')
            for row in reader:
                if row[0] == "HTTP" and row[1] != "None":
                    os_environ['HTTP_PROXY'] = row[1]
                if row[0] == "HTTPS" and row[1] != "None":
                    os_environ['HTTPS_PROXY'] = row[1]
                if row[0] == "Token" and row[1] != "None":
                    token = row[1]
    except FileNotFoundError:
        print("No private settings found")
    return token


if __name__ == "__main__":
    main()
