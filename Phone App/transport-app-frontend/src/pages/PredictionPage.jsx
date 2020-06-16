import React, { memo } from "react";
import { View, Text, StyleSheet, TouchableOpacity } from "react-native";

const PredictionPage = ({navigation}) => {

  const _onBackPressed = () => {
    navigation.navigate("HomePage");
  };


  const params = navigation.state.params
  const estTime = params['estTime']
  const estTimeMinutes = (parseFloat(estTime) / 60).toFixed(2)

  return (
    <View style={styles.container}>
        <Text style={styles.title}> 
            The predicted journey time is: 
        </Text>
        <Text style={styles.predText}>
            {estTimeMinutes} minutes
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
        marginTop: 300
    },
    button: {
        backgroundColor: "#6fded8",
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
    title: {
        fontSize: 30
    },
    predText: {
        fontSize: 30,
        color: "#7a7d7d"
    }
});


export default memo(PredictionPage);
