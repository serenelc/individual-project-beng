import React, { memo, useState } from "react";
import { View, Text, StyleSheet, TouchableOpacity } from "react-native";
import {TextInput } from 'react-native-paper';

const HomePage = ({ navigation }) => {
  const [from, setFrom] = useState({ value: "", error: "" });
  const [to, setTo] = useState({ value: "", error: "" });
  const [route, setRoute] = useState({ value: "", error: "" });
  const API_URL = "http://192.168.1.42:5000"

  const busStopValidator = (stopName) => {
    if (!stopName || stopName.length <= 0) return 'Stop name cannot be empty.'
    return ''
  };

  const routeValidator = (route) => {
    const regex = /^\d+$/
    if (!route || route.length <= 0) return 'Route cannot be empty.'
    if (!regex.test(route)) return 'Route must be a number'
    return ''
  };

  const _onGoPressed = () => {
    const fromError = busStopValidator(from.value);
    const toError = busStopValidator(to.value);
    const routeError = routeValidator(route.value);

    if (fromError || toError || routeError) {
      setFrom({ ...from, error: fromError });
      setTo({ ...to, error: toError });
      setRoute({ ...route, error: routeError });
      return;
    }
    
    fetch(API_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        from: from.value,
        to: to.value,
        route: route.value,
      }),
    })
      .then((res) => res.json())
      .then((res) => {

        if (res['success']) {
          const predTime = res['time']
          console.log("Navigate away!")
          
          navigation.navigate("PredictionPage", {estTime: predTime});

        } else if (res['fromError']) {
          setFrom({ ...from, error: "Please enter a valid stop" });
        } else {
          setTo({ ...to, error: "Please enter a valid stop" });
        }

      });
  }; 

  return (
    <View style={styles.container}>
        <Text style={styles.title}> 
            Find out how long your bus journey will take 
        </Text>
        
        <TextInput
            label="start stop"
            mode="outlined"
            textContentType="name"
            value={from.value}
            onChangeText={text => setFrom({ value: text, error: "" })}
            error={!!from.error}
            errorText={from.error}
            style={styles.textBox}
        />
        
        <TextInput
            label="destination stop"
            mode="outlined"
            textContentType="name"
            value={to.value}
            onChangeText={text => setTo({ value: text, error: "" })}
            error={!!to.error}
            errorText={to.error}
            style={styles.textBox}
        />

        <TextInput
            label="bus route"
            mode="outlined"
            value={route.value}
            onChangeText={text => setRoute({ value: text, error: "" })}
            error={!!route.error}
            errorText={route.error}
            style={styles.textBox}
        />

        <TouchableOpacity onPress={_onGoPressed} style= {styles.button}>
            <Text style={styles.buttonText}>Go</Text>
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


export default memo(HomePage);
