# Read_Write module

def read():
    pass


def write(df, directory_to_write):
    df.write.csv(directory_to_write, header=True, mode='overwrite')
    return
