import wandb

USER = 'kat-lon'
PROJECT = 'DDoS_sweeps'


sweep_config = {
    'method': 'bayes'
    }
 
#Definimos métrica
metric = {
    'name': 'test_acc',
    'goal': 'maximize'  
    }
 
sweep_config['metric'] = metric
 
parameters_dict = {
    'model': {
        'values': ['anx_04']
        },
    'learning_rate': {
        'distribution': 'uniform',
        'min': 0.000001,
        'max': 0.01
        },
    'batch_size': {
        'value': 512
      },
      'epochs': {
          'values': [n for n in range(30, 100, 10)]
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

sweep_config['program'] = 'train_sweeper.py'

sweep_id = wandb.sweep(sweep_config, project=PROJECT)
print(f'SWEEP_ID: {USER}/{PROJECT}/{sweep_id}')
