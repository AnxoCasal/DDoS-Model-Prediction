import train_sweeper
import wandb

sweep_config = {
    'method': 'bayes'
    }
 
#Definimos métrica
metric = {
    'name': 'loss',
    'goal': 'minimize'  
    }
 
sweep_config['metric'] = metric
 
parameters_dict = {
    'model': {
        'value': 'LNN_01'
        },
    'optimizer': {
        'value': 'adam'
        },
    'learning_rate': {
        'distribution': 'uniform',
        'min': 0.000001,
        'max': 0.01
        },
    'batch_size': {
        'values': [64, 32, 128, 256, 512]
      },
      'epochs': {
          'values': [n for n in range(20, 50, 5)]
      },
      'k': {
          'values': [5, 10]
      },
      'weight_decay': {
        'distribution': 'uniform',
        'min': 0,
        'max': 0.01
        }
    }
 
sweep_config['parameters'] = parameters_dict

list_opciones = ['anx_01', 'anx_02', 'anx_03', 'anx_04']
 
for o in list_opciones:
   
    sweep_config['parameters']['model']['value'] = o
    sweep_id = wandb.sweep(sweep_config, project="test_new_sweeper")
    wandb.agent(sweep_id, train_sweeper.train, count=10)