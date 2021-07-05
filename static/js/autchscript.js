console.log('JS OK')

async function test_response(){
    let url = 'http://localhost:5000/get'
    let response = await fetch(url);
    let result = await response.json()
    console.log(result)

     // добавление на html информации
    let div = document.createElement('div');
    div.className = "alert";
    //Вытаскивание из get информации по таблице. Прилетает array
    div.innerHTML = "<strong>JSON ответ от FLASK!</strong> <br> " + result[0][0];
    document.body.append(div);

}