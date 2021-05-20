import os

FORMATS = [
    "csv", "hdf"
]


class CBM3ResultsFileWriter:

    def __init__(self, format, out_path, writer_kwargs):
        """Create object to append dataframes to file

        Args:
            format (str): either "csv" or "hdf"
            out_path (str): Location into which DataFrames will be appended.
                For csv format this is a directory, and for hdf this is a
                filename.
            writer_kwargs (dict): extra keyword arguments to pass to the
                underlying pandas write methods

        Raises:
            ValueError: the specified format string does not match one of the
                supported formats.
        """
        if format not in FORMATS:
            raise ValueError(
                f"format must be one of: {FORMATS}")

        self.format = format
        if format == "csv":
            self.out_dir = out_path
        else:
            self.out_dir = os.path.dirname(out_path)
        self.out_path = out_path
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)
        self.created_files = set()
        self.writer_kwargs = writer_kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _def_get_file_path(self, name):
        if self.format == "csv":
            return os.path.join(self.out_dir, f"{name}.csv")
        else:
            return self.out_path

    def write(self, table_name, df):
        """Append the specified dataframe associated with the specified
        table_name to file.

        On the first call to this function for a given table any existing
        table will be overwritten, and a new file will be initialized.  On
        subsequent calls with the same table name, the specified data will
        be appended to the corresponding file output.

        Args:
            table_name (str): the name of the table to write
            df (pandas.DataFrame): the data to write
        """
        out_path = self._def_get_file_path(table_name)
        if out_path not in self.created_files:
            if os.path.exists(out_path):
                os.remove(out_path)
            self.created_files.add(out_path)
        args = [out_path]

        if self.format == "csv":
            kwargs = dict(
                mode='a', index=False,
                header=not os.path.exists(out_path))
            if self.writer_kwargs:
                kwargs.update(self.writer_kwargs)
            df.to_csv(*args, **kwargs)
        elif self.format == "hdf":
            kwargs = dict(key=table_name, mode="a", format='t', append=True)
            if self.writer_kwargs:
                kwargs.update(self.writer_kwargs)
            df.to_hdf(*args, **kwargs)
