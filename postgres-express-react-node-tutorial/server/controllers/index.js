// we're going to be exporting our controllers here.
// Helps consolidate my imports (require statements) 
// from once central place.


const todos = require('./todos');
const todoItems = require('./todoitems');

module.exports = {
  todos,
  todoItems,
};