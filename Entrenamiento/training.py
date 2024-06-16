import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
from sklearn.model_selection import train_test_split

class DataPreprocessing():
    
    @staticmethod
    def stratified_split_dataframe(df, target):
        
        X = df.drop(target, axis=1)
        y = df[target]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.15, stratify=y, random_state=2)

        train_df = pd.concat([X_train, y_train], axis=1)
        test_df = pd.concat([X_test, y_test], axis=1)
        
        return train_df, test_df
    
    @staticmethod
    def apply_scaler(df, categorical_cols, scaler=None):

        if scaler is None:
            scaler = MinMaxScaler()

        cols = [col for col in df.columns if col not in categorical_cols]
        df[cols] = scaler.fit_transform(df[cols])

        return df

    @staticmethod
    def apply_lda(df, target, n_components):
        
        X = df.drop(target, axis=1)
        y = df[target]
        
        lda = LDA(n_components=n_components)
        lda.fit(X, y)

        X = lda.transform(X)

        X = pd.DataFrame(X)
        
        return pd.concat([X,y], axis=1)

    @staticmethod
    def lda_variance_ratio(X, y):
        variance_ratio = {}

        n_features = X.shape[1]
        n_classes = len(set(y))
        max_components = min(n_features, n_classes - 1)

        for i in range(1, max_components + 1):
            lda = LDA(n_components=i)
            lda.fit(X, y)
            explained_variance = lda.explained_variance_ratio_
            variance_ratio[i] = explained_variance.sum()