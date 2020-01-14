from os            import pardir            as os_pardir
from os            import path              as os_path
from pandas        import DataFrame         as pd_DataFrame
from pandas        import read_csv          as pd_read_csv
from quandl        import get               as q_get
from quandl.errors import quandl_error


class Argument:

    def __init__(self,
                 id,
                 start,
                 end,
                 prefix=None,
                 drop=None,
                 rdiff="none",
                 collapse="none"):
        """
        An Argument object which contains the information
        to construct a request to send to quandl.
        id      : the id of the data set
        start   : the start date
        end     : the end date
        prefix  : the database prefix
        drop    : the columns to drop from the dataframe
        rdiff   : the transformation to do (usually percentage change)
        collapse: the frequency of data to download
        return  : none
        """
        self.id = id
        self.start = start
        self.end = end
        self.transformation = rdiff
        self.collapse = collapse
        self.prefix = prefix
        # The default drop columns for Google Finance data
        if drop is None:
            drop = ["High", "Low", "Open", "Volume", "Adjusted Close", ""]
        self.drop = drop

    def to_string(self):
        unique_id  = "Cache"
        unique_id += " id=" + self.id
        unique_id += " start=" + self.start
        unique_id += " end=" + self.end
        unique_id += " trans=" + self.transformation
        unique_id += ".csv"
        return unique_id.replace("\\", "-").replace("/", "-")


class QuandlInterface:

    def __init__(self, api_key):
        """
        An interface for downloading data from quandl.
        api_key: [YOUR API KEY] (taken from the .private.csv file)
        """
        self.api_key = api_key

    def download_data_set(self, argument):
        """
        This method tries to fetch a data set from quandl.
        argument: an argument object which contains the information to construct the request
        return  : a pandas DataFrame containing the data
        """
        assert isinstance(argument, Argument)
        data_frame = None
        try:
            data_set_name = argument.id
            if argument.prefix is not None:
                data_set_name = argument.prefix + data_set_name
            data_frame = q_get(data_set_name,
                               authtoken=self.api_key,
                               trim_start=argument.start,
                               trim_end=argument.end,
                               transformation=argument.transformation,
                               collapse=argument.collapse)
            assert isinstance(data_frame, pd_DataFrame)
            for d in argument.drop:
                try:
                    data_frame = data_frame.drop(d, axis=1)
                except:
                    continue
        except quandl_error.AuthenticationError:
            print("AuthenticationError")
        except quandl_error.ColumnNotFound:
            print("ColumnNotFound")
        except quandl_error.ForbiddenError:
            print("ForbiddenError")
        except quandl_error.InternalServerError:
            print("InternalServerError")
        except quandl_error.InvalidDataError:
            print("InvalidDataError")
        except quandl_error.InvalidRequestError:
            print("InvalidRequestError")
        except quandl_error.LimitExceededError:
            print("LimitExceededError")
        except quandl_error.NotFoundError:
            print("NotFoundError")
        except quandl_error.ServiceUnavailableError:
            print("ServiceUnavailableError")
        if data_frame is None:
            raise Exception("Data Set Not Initialized", argument.id)
        else:
            return data_frame

    def get_data_set(self, argument):
        file_name = argument.to_string()
        basepath = os_path.dirname(__file__)
        path = os_path.abspath(os_path.join(basepath, os_pardir, "market_data", file_name))
        try:
            data_frame = pd_read_csv(path)
            data_frame = data_frame.set_index("Date")
            return data_frame
        except:
            data_frame = self.download_data_set(argument)
            data_frame.to_csv(path, mode="w+")
            return data_frame

    def get_data_sets(self, arguments):
        """
        This method calls the get_data_set() method
        to download and join various data sets.
        arguments: a list of Argument objects
        return   : a pandas DataFrame
        """
        # assert isinstance(arguments, [Argument])
        combined_data_frame = None
        for arg in arguments:
            assert isinstance(arg, Argument)
            arg_data_frame = self.get_data_set(arg)
            new_columns = []
            for i in range(len(arg_data_frame.columns)):
                new_columns.append(arg.id + "_" + arg_data_frame.columns[i])
            arg_data_frame.columns = new_columns
            if combined_data_frame is None:
                combined_data_frame = arg_data_frame
            else:
                combined_data_frame = combined_data_frame.join(arg_data_frame)
        combined_data_frame = combined_data_frame.dropna()
        return combined_data_frame
