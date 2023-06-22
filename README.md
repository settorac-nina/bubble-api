<div align="center">
    <h1><code>Bubble Api</code></h1>
    <p><strong>Interactions with Bubble.io API in Python made easy !</strong></p>
    <img alt="Python Badge" src="https://img.shields.io/badge/-Python-D6D6D6?logo=python"/>
    <img alt="PiPY Badge" src="https://img.shields.io/pypi/v/bubble-api"/>
    <img alt="CI/CD Badge" src="https://github.com/settorac-nina/bubble-api/actions/workflows/cicd.yaml/badge.svg?branch=main)"/>
    <img src="https://img.shields.io/badge/Made%20With-Love-ef7d16.svg"/>
    <p>
        <a href="https://cuure.com?utm_source=github&utm_medium=referral">
            Powered by <strong>Cuure</strong>
        </a>
        &nbsp;&nbsp;
        <a href="https://cuure.com?utm_source=github&utm_medium=referral">
            <img src="https://github.com/settorac-nina/bubble-api/blob/readme_improvement/assets/cuure_logo.gif" width="60" height="60" align="center"/>
        </a>
    </p>
</div>

---

## Installing

The easiest way to install it is through **pipy** :

```shell
pip install bubble-api
```

## How to use it

At the moment, there is no documentation for this library.
You can, however, rely on the integration tests we use to ensure proper integration with Bubble.
You can find them [here](tests/integration).

### Creating a BubbleWrapper instance :

```python
from bubble_api import BubbleWrapper

bubble_wrapper = BubbleWrapper(
    base_url="https://cuure.com",
    api_token="YOUR_API_TOKEN",
    bubble_version="live"
)
```

### Interact with bubble data :

From the `BubbleWrapper` instance you can now interact with the data api easily.

```python
object_data = bubble_wrapper.get(
    "table_name",
    bubble_id="bubble_object_id",
)
```

And so on with all the basic interactions with the api: **delete**, **update**, **replace**, ...

You can also use constraints for getting or deleting objects.
This is done through a list of `Contraint` object you can declare by operations with the `Field` class.

```python
from bubble_api import BubbleWrapper, Field
from datetime import datetime

bubble_wrapper = BubbleWrapper(...)

constraints = [
    Field("name") == "Bob",
    Field("Created") > datetime(2023, 1, 1),
]

data = bubble_wrapper.get(
    "table_name",
    constraints=constraints
)
```

All the fields names and constraints values formatting is handle by the library.

## Incoming features

- [ ] Big request optimization using remaining approximation (exclude_remaining)
- [ ] Multithreading
- [ ] Default fields (Modified Date, unique id, Created Date, ...)
- [ ] Lifetime object management (update, refresh, replace)
- [ ] Documentation

## Authors

* **Dylan Nina** - *Initial work* - [settorac-nina](https://github.com/settorac-nina)
* **Mathis Bourdin** - *Contributor* - [mathisbrdn](https://github.com/mathisbrdn)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
