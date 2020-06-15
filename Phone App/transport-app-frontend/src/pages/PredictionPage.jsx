import React, { memo } from "react";
import { View, Text, StyleSheet, TouchableOpacity } from "react-native";

const PredictionPage = ({ route, navigation, estTime }) => {

  const _onBackPressed = () => {
    navigation.navigate("HomePage");
  };

  console.log(route)
  console.log(estTime)
//   const { estTime } = route.params;
  console.log("On prediction page! ", estTime)

  return (
    <View style={styles.container}>
        <Text style={styles.title}> 
            The predicted journey time is: {estTime}
        </Text>

        <TouchableOpacity onPress={_onBackPressed} style= {styles.button}>
            <Text style={styles.buttonText}>Back</Text>
        </TouchableOpacity>

    </View>

  );
};

const styles = StyleSheet.create({
    container: {
        margin: 16,
        marginTop: 100
    },
    button: {
        backgroundColor: "#99ff99",
        padding: 20,
        borderRadius: 5,
        marginTop: 20,
        width:200,
        alignSelf: "center"
    },
    buttonText: {
        fontSize: 20,
        color: '#000',
        alignSelf: "center"
    }, 
    textBox: {
        marginTop: 20,
        textAlignVertical: "center"
    },
    predText: {
        padding: 50,
        backgroundColor: "#9966ff"
    },
    title: {
        fontSize: 30
    }
});


export default memo(PredictionPage);
