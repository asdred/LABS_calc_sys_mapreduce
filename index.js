const mongoose = require('mongoose');
const fs = require('fs');

mongoose.connect('mongodb://localhost/calcsys');

let ApartmentSchema = mongoose.Schema({
  district: String,
  cost: Number,
});
let Apartment = mongoose.model('Apartment', ApartmentSchema);
let map = function () { 
    emit(this.district, {cost: this.cost, count: 1}) 
};
let reduce = function (key, values) { 
    var cost = 0;
	var count = 0;
	for(var i in values){
		count += values[i].count;
		cost += values[i].cost;
	}
	return {cost: cost, count: count};
};
let finalize = function (key, reducedValue) {
	return reducedValue.cost / reducedValue.count;
};

mongoose.connection.on('error', console.error.bind(console, 'connection error:'));

mongoose.connection.once('open', () => {
    mongoose.connection.db.listCollections().toArray((err, names) => {
      if (err) {
        console.log(err);
      } else {
        let index = names.findIndex(x => x.name === 'apartments');
        
        if (index !== -1) {
            console.log('Данные присутствуют в базе', '\nПрименение MapReduce');
            Apartment.mapReduce({ map, reduce, finalize }, function (err, results) {
                console.log(results, err);
                mongoose.connection.close();
            })
        } else {
            console.log('Данные не найдены, поиск в: ./data_input/apartments.json');
            
            try {
                let apartments = JSON.parse(require('fs').readFileSync('./data_input/apartments.json', 'utf8'));

                console.log('Данные найдены, добавление в базу');
                
                Apartment.insertMany(apartments, (err, data) => {
                    console.log('Добавлено ' + data.length + ' объектов', '\nПерезапустите для применения MapReduce');
                    mongoose.connection.close();
                });
                
            } catch (e) {
                if (e.code = 'ENOENT') console.log('Файл отсутствует: ./data_input/apartments.json');
            }
        }
      }
    });
});