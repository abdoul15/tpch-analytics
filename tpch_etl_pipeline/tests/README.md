# Tests pour le projet TPCH Data Pipeline

Ce répertoire contient les tests unitaires et d'intégration pour le projet TPCH Data Pipeline.

## Structure des tests

```
tests/
├── conftest.py                  # Configuration partagée pour pytest
├── unit/                        # Tests unitaires
│   ├── bronze/                  # Tests pour la couche bronze
│   │   ├── test_customer.py
│   │   └── ...
│   ├── silver/                  # Tests pour la couche silver
│   │   ├── test_dim_customer.py
│   │   └── ...
│   ├── gold/                    # Tests pour la couche gold
│   │   ├── test_daily_sales_metrics.py
│   │   └── ...
│   └── utils/                   # Tests pour les utilitaires
│       ├── test_base_table.py
│       └── ...
└── integration/                 # Tests d'intégration
    ├── test_bronze_to_silver.py # Tests d'intégration entre couches
    └── test_end_to_end.py       # Test du pipeline complet
```

## Exécution des tests

Les tests peuvent être exécutés à l'aide des commandes Makefile suivantes:

```bash
# Exécuter tous les tests
make test

# Exécuter uniquement les tests unitaires
make test-unit

# Exécuter uniquement les tests d'intégration
make test-integration

# Exécuter les tests avec couverture de code
make test-coverage
```

## Fixtures

Les fixtures pour les tests sont définies dans le fichier `conftest.py`. Elles incluent:

- `spark`: Une session Spark pour les tests
- `sample_customer_data`, `sample_nation_data`, etc.: Des données de test pour chaque table
- `temp_storage_dir`: Un répertoire temporaire pour stocker les données Delta pendant les tests

## Types de tests

### Tests unitaires

Les tests unitaires vérifient le comportement de chaque classe ETL individuellement. Ils utilisent des mocks pour simuler les dépendances et se concentrent sur la logique de transformation.

### Tests d'intégration

Les tests d'intégration vérifient l'interaction entre les différentes couches du pipeline. Ils testent le flux de données de bout en bout, de l'extraction à la transformation et au chargement.

## Bonnes pratiques

1. **Isolation**: Chaque test doit être indépendant des autres tests.
2. **Mocks**: Utiliser des mocks pour simuler les dépendances externes.
3. **Assertions**: Vérifier non seulement que le code s'exécute sans erreur, mais aussi que les résultats sont corrects.
4. **Nettoyage**: Nettoyer les ressources temporaires après les tests.
5. **Couverture**: Viser une couverture de code élevée pour les tests unitaires.
