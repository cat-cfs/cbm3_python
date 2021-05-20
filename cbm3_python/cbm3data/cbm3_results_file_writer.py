import os

FORMATS = [
    "csv", "hdf"
]


class CBM3ResultsFileWriter:

    def __init__(self, format, out_path, writer_kwargs):
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

    def write(self, name, df):
        out_path = self._def_get_file_path(name)
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
            raise NotImplementedError("hdf not yet implemented")
            # this does not currently work! look into
            # https://pandas.pydata.org/docs/reference/api/pandas.HDFStore.append.html
            # pandas.HDFStore.append
            kwargs = dict(key=name, mode="a")
            if self.writer_kwargs:
                kwargs.update(self.writer_kwargs)
            df.to_hdf(*args, **kwargs)
