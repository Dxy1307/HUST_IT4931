# HUST_IT4931

***Đại học Bách Khoa Hà Nội***

***Môn học: Lưu trữ và xử lý dữ liệu lớn***

***Nhóm: 13***

| STT | Họ và Tên            | Mã Sinh Viên | Email                        |
|-----|----------------------|--------------|-------------------------------|
| 1   | [Nguyễn Đình Tùng](https://www.facebook.com/ahihitungoi)      | 20215663     | tung.nd215663@sis.hust.edu.vn
| 2   | [Nguyễn Đức Duy](https://www.facebook.com/Dxy.1307)        | 20210275     | duy.nd210275@sis.hust.edu.vn
| 3   | [Đào Anh Quân](https://www.facebook.com/profile.php?id=100013755369820)          | 20215631     | quan.da215631@sis.hust.edu.vn
| 4   | [Vũ Thị Thanh Hoa](https://www.facebook.com/profile.php?id=100062067740273)      | 20210356     | hoa.vtt210356@sis.hust.edu.vn
| 5   | [Ngân Văn Thiện](https://www.facebook.com/thien.nganvan.16)        | 20215647     | thien.nv215647@sis.hust.edu.vn

## About dataset

### About
This file contaisn behavior data for 7 months (from October 2019 to April 2020) from a large multi-category online store.

Each row in the file represents an event. All events are related to products and users. Each event is like many-to-many relation between products and users.

### How to read it
There are different types of events. See below.

Semantics (or how to read it):

> User **user_id** during session **user_session** added to shopping cart (property **event_type** is equal **cart**) product **product_id** of brand **brand** of category **category_code** (**category_code**) with price **price** at **event_time**

### File structure

| Property          | Description
|-------------------|----------------------|
| event_time        | Time when event happened at (in UTC).
| event_type        | Only one kind of event: purchase.
| product_id        | ID of a product.
| category_id       | Product's category ID.
| category_code     | Product's category taxonomy (code name) if it was possible to make it. Usually present for meaningful categories and skipped for different kinds of accessories.
| brand             | Downcased string of brand name. Can be missed.
| price             | Float price of a product. Present.
| user_id           | Permanent user ID.
| user_session      | Temporary user's session ID. Same for each user's session. Is changed every time user come back to online store from a long pause.

### Event types

Events can be:
- *view* - a user viewed a product
- *cart* - a user added a product to shopping cart
- *remove_from_cart* - a user removed a product from shopping cart
- *purchase* - a user purchased a product

### Multiple purchases per session

A session can have multiple **purchase** events. It's ok, because it's a single order.

### How to download

You can download this dataset from [Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data).

### Many thanks

Thanks to [Michael Kechinov](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data).

Thanks to [REES46 Marketing Platform](https://rees46.com/) for this dataset.

## Project

### Simulate incoming data in real time

![data_realtime_simulation](img/data_realtime_simulation.png)

# Dxy