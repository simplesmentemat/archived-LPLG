from polars import LazyFrame
import os

class TeamDataSaver:
    def __init__(self, lazy_frame: LazyFrame, folder_path: str):
        self.lazy_frame = lazy_frame
        self.folder_path = folder_path
    
    def write_csv(self, filename="team_Details.csv"):
        """
        Saves the LazyFrame as a CSV file in the specified folder.

        Args:
            filename (str, optional): The name of the file to be saved. Defaults to "team_Details.csv".

        Returns:
            str: Confirmation message with the path where the file is saved.
        """
        path = os.path.join(self.folder_path, filename)
        self.lazy_frame.write_csv(path)
        return f"CSV file saved at {path}"

    def write_parquet(self, filename="team_Details.parquet"):
        """
        Saves the LazyFrame as a Parquet file in the specified folder.

        Args:
            filename (str, optional): The name of the file to be saved. Defaults to "team_Details.parquet".

        Returns:
            str: Confirmation message with the path where the file is saved.
        """
        path = os.path.join(self.folder_path, filename)
        self.lazy_frame.write_parquet(path)
        return f"Parquet file saved at {path}"

class PlayerDetailsSaver:
    def __init__(self, df_list, folder_path):
        self.df_list = df_list
        self.folder_path = folder_path

    def write_csv(self):
        """
        Saves each DataFrame in the list as a separate CSV file in the specified folder.

        Returns:
            str: A concatenated string of confirmation messages for each saved file.
        """
        saved_paths = []
        for df_name, df in self.df_list:
            data_saver = TeamDataSaver(df, self.folder_path)
            save_message = data_saver.write_csv(f'{df_name}.csv')
            saved_paths.append(save_message)
        return '\n'.join(saved_paths)

    def write_parquet(self):
        """
        Saves each DataFrame in the list as a separate Parquet file in the specified folder.

        Returns:
            str: A concatenated string of confirmation messages for each saved file.
        """
        saved_paths = []
        for df_name, df in self.df_list:
            data_saver = TeamDataSaver(df, self.folder_path)
            save_message = data_saver.write_parquet(f'{df_name}.parquet')
            saved_paths.append(save_message)
        return '\n'.join(saved_paths)

    def __repr__(self):
        """
        Returns a string representation of the object, listing each DataFrame name and its contents.

        Returns:
            str: A string representation of the PlayerDetailsSaver object.
        """
        df_strings = []
        for df_name, df in self.df_list:
            df_string = f"{df_name}:\n{df}"
            df_strings.append(df_string)
        return "\n\n".join(df_strings)

