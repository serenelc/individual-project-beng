import { createAppContainer } from 'react-navigation';
import { createStackNavigator } from 'react-navigation-stack';
import {HomePage, PredictionPage} from './pages';

const Router = createStackNavigator(
  {
    HomePage,
    PredictionPage
  },
  {
    initialRouteName: 'HomePage',
    headerMode: 'none',
  }
);

export default createAppContainer(Router);
