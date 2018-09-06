package com.zhengxuetao

class Phone(id: Int, name: String) {
    var phone_id: Int = id
    var phone_name: String = name

    def show(): Unit = {
        printf(phone_name + phone_id)
    }

    def this(id: Int) {
        this(id, "default")
    }

    def this(id: Int, name: String, other: String) {
        this(id, other)
    }

}
