import React from "react";
import ReactDOM from "react-dom";

class Mytweet extends React.Component {
  constructor (props)
  {
  super(props);
  this.state ={
  tweet:'',
  imageUploaded: false
  };
}
handleClick=event=>{
this.setState(state => ({
imageUploaded: !this.state.imageUploaded
}));
}
handleKeyUp=event=>{
  this.setState({
    tweet: event.target.value
  });
  }
render() {
const myCount=this.state.tweet.length + (this.state.imageUploaded ? 30 : 0);
return ( <div className="myTweet">
<textarea placeholder="Remember, be nice!" id='myTextarea' cols="30" rows="5" onKeyUp={this.handleKeyUp}></textarea>
<label id="myText" style = {{color : myCount > 300 ?  '#FF0000' : '#000000'}}>{myCount}</label>/300
<br/>
<button id="image" onClick={this.handleClick}>{this.state.imageUploaded ? 'Remove_Image' : 'Upload_Image'}</button>
<button id="tweet" disabled={myCount > 300}>post!</button>
</div>
);
}
};
ReactDOM.render(<Mytweet/>,
 document.getElementById('root'));