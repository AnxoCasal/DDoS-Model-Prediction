import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
from sklearn.model_selection import train_test_split

class DataPreprocessing():
    
    @staticmethod
    def stratified_split_dataframe(df, target):
        """
        Divide el dataset de forma estratificada segun la columna objetivo
        
        :df: Dataframe
        :target: columna objetivo sobre la que hacer la división
        """
        
        X = df.drop(target, axis=1)
        y = df[target]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.15, stratify=y, random_state=2)

        train_df = pd.concat([X_train, y_train], axis=1)
        test_df = pd.concat([X_test, y_test], axis=1)
        
        return train_df, test_df
    
    @staticmethod
    def apply_scaler(df, categorical_cols, scaler=None):
        """
        Aplica el MinMaxScaler a todas las columnas del dataset, ingorando unicamente las indicadas como categoricas. Además se puede pasar un scaler ya creado para utilizarlo

        :df: Dataframe
        :categorical_cols: Columnas marcadas como categóricas que serán ignoradas por el scaler
        :scaler: Objeto scaler que se usara si existe, y en caso de que no se creará uno nuevo
        """

        if scaler is None:
            scaler = MinMaxScaler()

        cols = [col for col in df.columns if col not in categorical_cols]
        df[cols] = scaler.fit_transform(df[cols])

        return df

    @staticmethod
    def apply_lda(df, target, n_components):
        """
        Aplica la reduccion de dimecionalidad al dataset en base a una columna objetivo. Reduce las columnas que no séan objetivo en base al número que se le indique
        
        :df: Dataframe
        :target: Columna objetivo por la que se va a guiar para reducir el resto
        :n_components: Numero de columnas a la que se quiere reducir
        """
        
        X = df.drop(target, axis=1)
        y = df[target]
        
        lda = LDA(n_components=n_components)
        lda.fit(X, y)

        X = lda.transform(X)

        X = pd.DataFrame(X)
        
        return pd.concat([X,y], axis=1)

    @staticmethod
    def lda_variance_ratio(X, y):
        """
        Comprueba como varía la información según la cantidad de columnas a las que se reduzca usando el algoritmo LDA
        """
        variance_ratio = {}

        n_features = X.shape[1]
        n_classes = len(set(y))
        max_components = min(n_features, n_classes - 1)

        for i in range(1, max_components + 1):
            lda = LDA(n_components=i)
            lda.fit(X, y)
            explained_variance = lda.explained_variance_ratio_
            variance_ratio[i] = explained_variance.sum()

        return variance_ratio