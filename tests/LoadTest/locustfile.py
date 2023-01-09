from locust import HttpUser, task

class HelloWorldUser(HttpUser):
    @task
    def hello_world(self):
        self.client.get("/sql/test/select/flight")
        self.client.get("/sql/test/select/account")