import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import pandas as pd
from torch.utils.data import DataLoader, Subset
from Entrenamiento.Modelos.LNN_01 import LNN_01
from sklearn.model_selection import StratifiedKFold, StratifiedShuffleSplit
import wandb

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
DATASET_PATH = './Entrenamiento/training_set,parquet'

def train(config=None):
    '''
    Entrenador usado por el agente de wandb. Recibe el diccionari config con la siguiente
    estructura:

        model:
        optimizer:
        learning_rate:
        epochs:
        batch_size:
        k:
    '''
    # Initialize a new wandb run
    with wandb.init(config=config):
        # If called by wandb.agent, as below,
        # this config will be set by Sweep Controller
        config = wandb.config

        train_data, test_data = build_data()
        model = build_model(config.model)
        optimizer = optim.Adam(model.parameters(), lr=config.learning_rate, weight_decay=config.weight_decay)
        criterion = nn.CrossEntropyLoss()

        kf = StratifiedKFold(n_splits=config.k, shuffle=True, random_state=2)
        epochs_per_fold = config.epochs/config.k
        factor = 0
        
        for train_idx, test_idx in kf.split(train_data.data, train_data.targets):
            train_subset = Subset(train_data, train_idx)
            test_subset = Subset(train_data, test_idx)
            
            train_loader = DataLoader(train_subset, batch_size=config.batch_size, shuffle=True)
            test_loader = DataLoader(test_subset, batch_size=config.batch_size, shuffle=True)
            
            factor += 1
            training_loop(model, train_loader, test_loader, optimizer, criterion, epochs_per_fold, factor=factor)

        plot_final(model, test_data)

    wandb.finish(quiet=True)


def build_data():
    '''
    Divide el parquet en train y test set de forma estratificada.

    Retornará 2 torch.DataLoader, uno para el conjunto de prueba y otro para el de entrenamiento.
    '''
    strat_splitter = StratifiedShuffleSplit(n_splits=1, test_size=0.15, random_state=2)

    df = pd.read_parquet(DATASET_PATH)

    X = df.drop('Label_Index', axis=1)
    y = df['Label_Index']

    train_idx, test_idx = strat_splitter.split(X, y)

    train_data = Subset(train_data, train_idx)
    test_data = Subset(train_data, test_idx)

    return 


def build_model(model):
    '''
    Crea el modelo en función de los parámetros indicados en wandb.config
    y lo mueve a device

    VALORES:
        -cnn: empleará el modelo convolucional simple
        -res: empleará el modelo de redes residuales
    '''
    if model == "LNN_01":
        model = LNN_01()
    elif model == "LNN_02":
        model = LNN_02()

    model.to(DEVICE)
    return model


def train_epoch(model, loader, optimizer, criterion):
    '''
    Constituye un epoch del entrenamiento y retorna la loss para la iteración
    '''
    model.train()
    running_loss = 0
    for images, labels in loader:

        images = images.to(DEVICE)
        labels = labels.to(DEVICE)

        optimizer.zero_grad()
        outputs = model(images)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()

        running_loss += loss.item() * images.size(0)

    return running_loss / len(loader.dataset)   

def calculate_accuracy(model, loader):
    '''
    Calcula la accuracy basada en el loader recibido
    '''
    model.eval()
    correct_predictions = 0
    total_predictions = 0

    with torch.no_grad():
        for images, labels in loader:

            images = images.to(DEVICE)
            labels = labels.to(DEVICE)

            outputs = model(images)
            _, predicted = torch.max(outputs, 1)
            correct_predictions += (predicted == labels).sum().item()
            total_predictions += labels.size(0)

    accuracy = 100 * correct_predictions / total_predictions
    return accuracy

def training_loop(model, train_loader, val_loader, optimizer, criterion, epochs, factor=1):
    '''
    Función wrapper de calculate_accuracy y train_epoch. Actualizará los logs
    de wandb con la información pertinente a cada epoch.

    Recibe el valor especial factor, que nos servirá para identificar en que fold nos
    hayamos y así poder calcular el valor correcto de la epoch:
        Ej para el fold nº2 y epoch 3 de 5:

            epoch_actual + epochs(factor - 1) = 3 + 5(2 - 1) = 8

        Estariamos en la epoch 3 del segundo fold, lo que constituye la octava
        epoch del total del entrenamiento
    '''
    for epoch in range(epochs):
        loss = train_epoch(model, train_loader, optimizer, criterion)
        accuracy = calculate_accuracy(model, val_loader)
        epoch_total = epoch + epochs * (factor - 1)

        wandb.log({'epoch': epoch_total, 'loss': loss, 'val_acc':accuracy})

def plot_final(model, data):
    '''
    Descarga el test set y logea la accuracy final conseguida sobre el test_set.
    Además, crea una matriz de confusion, precision vs recall y una ROC curve 
    '''
    test_loader = DataLoader(data, batch_size=10000, shuffle=False)

    model.eval()
    with torch.no_grad():
        for inputs, labels in test_loader:

            inputs = inputs.to(DEVICE)
            labels = labels.to(DEVICE)

            predictions = model(inputs)
            scores = torch.softmax(predictions, dim=1).to('cpu')
            _, predicted = torch.max(predictions, 1)
            outputs = predicted.tolist()
            
            correct_predictions = (predicted == labels).sum().item()
            total_predictions = labels.size(0)

    accuracy = 100 * correct_predictions / total_predictions
    wandb.log({"test_acc": accuracy})

    ground_truth = data.targets

    wandb.log({"pr": wandb.plot.pr_curve(ground_truth, scores, labels=data.classes)})
    wandb.log({"roc": wandb.plot.roc_curve(ground_truth, scores, labels=data.classes)})

    cm = wandb.plot.confusion_matrix(
    y_true=data.targets, preds=outputs, class_names=data.classes
    )

    wandb.log({"conf_mat": cm})